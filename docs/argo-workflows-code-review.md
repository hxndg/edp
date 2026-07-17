# Argo Workflows 切换：逐行代码讲解

> 对应改动：worker pod 的监督从自研 `common/worker_pods.py`（PipesK8sClient + 线程池 + 监视线程）
> 切换为 Argo Workflows（`common/argo_workflows.py`）。本文逐行讲解新模块、部署配置、
> 以及两个调用方（ingest / 训练）的接缝，并附回归验证记录。
>
> 阅读前提：saga、错误码、staging 契约的细节见 `docs/pipes-fanout-code-review.md`——
> 那部分逻辑**一行没改**，本文只讲"pod 怎么被拉起和监督"这一段的新实现。

---

## 0. 切换前后对照：什么变了，什么没变

```text
                        Pipes 形态（旧）                    Argo 形态（新）
────────────────────────────────────────────────────────────────────────────
拉起 pod                PipesK8sClient 逐个 create        提交 1 个 Workflow CR，Argo 起 N 个 pod
批内并发                run pod 里 ThreadPoolExecutor      spec.parallelism，Argo controller 排队
                        手动切波（200 条切 10 波）
pod 超时                pod spec activeDeadlineSeconds     模板级 activeDeadlineSeconds（不变）
整批卡死兜底            线程 future 的 grace 超时 + 主动删 pod   workflow 级 activeDeadlineSeconds
saga 心跳               专门的 _WaveMonitor 后台线程        轮询循环里每个 tick 顺手回调（无线程）
pod 终态抓取            监视线程轮询 N 个 pod 存快照         终态后读 1 次 workflow.status.nodes
worker 日志             Pipes 流回 Dagster compute log     Argo archiveLogs 归档 MinIO + Argo UI
worker 依赖             镜像里要有 dagster-pipes            零依赖（纯命令行 + 环境变量）
────────────────────────────────────────────────────────────────────────────
不变：saga claim/advance/fail/succeed、manifest.json 数据契约、错误码三来源、
     watchdog 按码重试 + 内存升档、run pod 单写者 Iceberg commit、staging 布局
```

监视面从"N 个 pod"收敛成"1 个 CR"，run pod 里所有并发/线程代码消失，
worker 镜像可以是不含任何平台库的用户镜像。

---

## 1. `common/argo_workflows.py`（核心新模块，227 行）

### 1.1 模块 docstring 与常量（1–38 行）

```python
"""worker pod 的拉起/监视——Argo Workflows 形态（README 3.6.3，替代 worker_pods.py）。
...
"""
```

- **1–21 行**：设计摘要。五个要点——一批一 CR、提交幂等、单线程轮询即心跳、
  结果判定不变（manifest 优先）、日志归档。第 18–20 行专门声明**Argo 侧的
  retryStrategy 故意不启用**：重试语义仍归 saga/watchdog（业务层按错误码决定），
  Argo 只报告事实，不自作主张重跑——否则 OOM 的内存升档（依赖 saga attempt）
  就会被 Argo 的盲目原样重试架空。

```python
import hashlib      # 24 行：workflow 名字的内容摘要
import shlex        # 26 行：把 command list 安全拼成 shell 字符串
import time         # 27 行：轮询计时
from common.config import settings   # 30 行：namespace / 镜像名
from common.errors import ErrorCode  # 31 行：classify() 返回的码
```

- **35 行** `WORKER_LABEL = "edp-worker"`：打在 workflow 和 pod 上的 label，
  `kubectl get wf -l app=edp-worker` 一把捞出所有 EDP 批次。
- **36 行** `GROUP, VERSION, PLURAL = "argoproj.io", "v1alpha1", "workflows"`：
  Workflow CRD 的 API 坐标，`CustomObjectsApi` 的所有调用都要这三样。
- **37 行** `POLL_SECONDS = 5.0`：轮询间隔。也决定了 saga 心跳的刷新频率
  （5s ≪ `SAGA_TAKEOVER_MINUTES`，watchdog 不会误判）。
- **38 行** `_TERMINAL_PHASES = {"Succeeded", "Failed", "Error"}`：workflow 的
  三个终态。`Failed` = 有节点失败但 controller 正常收尾；`Error` = controller
  自身出问题（如模板渲染失败）。两者对调用方等价——都进"逐节点判定"。

### 1.2 `WorkerSpec`（41–47 行）：调用方给的"一个 worker 的描述"

```python
@dataclass
class WorkerSpec:
    upload_id: str          # 业务 id（upload_id 或训练 job_id），也是 outcome 的键
    staging_prefix: str
    memory_limit: str       # 按 saga attempt 升档（*_WORKER_MEMORY_TIERS）
    command: list[str] | None = None
```

- 与旧 `worker_pods.WorkerSpec` **字段完全相同**——这是刻意的：两个调用方
  只需要改一行 import，别的都不用动。
- `upload_id` 是"业务主键"角色：ingest 传 upload_id，训练传 job_id；
  返回的 `outcomes` 字典就以它为键。
- `memory_limit`：值由调用方算好传进来（`_memory_tier(attempt, tiers)`），
  本模块不关心升档逻辑，只负责把它放进 pod 的 resources。
- `command=None` 时用默认 ingest 解析入口（见 1.5 节 92–97 行）；训练链传
  自己的入口（`engines.worker.train_mock`）。

### 1.3 `PodOutcome`（50–69 行）：一个节点的最终观测

```python
@dataclass
class PodOutcome:
    upload_id: str
    phase: str | None = None       # Argo node phase
    message: str | None = None     # OOMKilled / deadline 等原因在这里
    exit_code: int | None = None   # node.outputs.exitCode
    workflow: str | None = None
    pod_names: list[str] = field(default_factory=list)
```

- 所有字段默认 None/空：**"pod 从没被观测到"本身是合法状态**（比如资源不足
  一直 Pending 到 workflow 超时），这时 phase=None，classify() 会给 WORKER_LOST。
- `workflow` / `pod_names` 是排查线索：拿它去 Argo UI 或
  `s3://lake/argo/{workflow}/{pod}/main.log` 找现场。

```python
    def classify(self) -> tuple[ErrorCode, str]:
        msg = self.message or ""
        if "OOMKilled" in msg or self.exit_code == 137:
            return ErrorCode.WORKER_OOM, ...
        if "deadline" in msg.lower():
            return ErrorCode.WORKER_TIMEOUT, ...
        detail = msg or f"phase={self.phase}（节点未观测到，pod 可能没调度上）"
        return ErrorCode.WORKER_LOST, ...
```

- **61–69 行**：错误码三来源里的"来源 2"（pod 级失败推断）。**只有调用方
  发现 staging 里没有 manifest.json 时才会调它**——有清单永远以 worker 自报为准。
- **64 行**：OOM 的两个信号取或——Argo 把容器终态原因写进 node message
  （内容含 `OOMKilled`）；exit_code 137 = 128+SIGKILL，是内核 OOM killer 的
  典型退出码，两个信号任一命中即判 OOM。判成 OOM 的意义在于 watchdog 重试时
  会升内存档，所以宁可信号宽一点。
- **66 行**：模板级 activeDeadlineSeconds 触发时 Argo 的 node message 含
  "Pod was active on the node longer than the specified deadline"，用小写
  `deadline` 子串匹配。
- **68–69 行**：兜底 WORKER_LOST——包括节点根本没出现（调度失败、workflow 级
  超时先到）。RETRY_POLICY 里 LOST 可自动重试（可能是环境抖动）。

### 1.4 `_custom_api` / `_workflow_name`（72–84 行）

```python
def _custom_api():
    import kubernetes
    try:
        kubernetes.config.load_incluster_config()
    except Exception:
        kubernetes.config.load_kube_config()
    return kubernetes.client.CustomObjectsApi()
```

- **73 行**：`kubernetes` 库函数内延迟导入——不跑 fan-out 的进程（gateway 等）
  import 本模块不付这个代价。
- **76–78 行**：先集群内配置（run pod 里，读 SA token），失败退回 kubeconfig
  （开发机直接跑）。与旧模块同款逻辑。
- Workflow 是 CRD，所以用 `CustomObjectsApi`（泛化的 dict-in/dict-out 接口），
  不是内置资源的 typed client。

```python
def _workflow_name(run_id: str, specs: list[WorkerSpec]) -> str:
    digest = hashlib.sha1(",".join(sorted(s.upload_id for s in specs)).encode()).hexdigest()[:6]
    return f"edp-{run_id[:8]}-{digest}".lower()
```

- **名字 = 幂等键**。`run_id 前 8 位 + 批内业务 id 排序后的 sha1 前 6 位`：
  - 同一个 run 重复提交同一批 → 同名 → 409 → 直接跟踪已有 workflow（不双跑）；
  - 同一个 run 里 ingest 分波调用多次 `launch_wave` → 批内容不同 → digest 不同 → 不冲突；
  - **重跑走的是新 Dagster run（新 run_id）→ 新名字**，与旧 CR 完全隔离，
    旧 CR 由 ttlStrategy 1 天后自动清。
- `sorted(...)`：调用方传入顺序不影响名字。`.lower()`：K8s 资源名必须小写。

### 1.5 `_build_workflow`（87–147 行）：批 → Workflow CR

这是整个模块的核心——把一批 `WorkerSpec` 渲染成一个 Workflow 自定义资源（纯 dict，
提交后 Argo controller 负责其余一切）。

```python
    tasks, args_list = [], []
    for i, s in enumerate(specs):
        command = s.command or [
            "python", "-m", "engines.worker.ingest_parse",
            "--upload-id", s.upload_id,
            "--run-id", run_id,
            "--staging-prefix", s.staging_prefix,
        ]
```

- **91–97 行**：每个 spec 生成一条命令行。`command=None` 用默认 ingest 入口，
  参数与旧 Pipes 形态完全一致（worker 侧 `ingest_parse.py` 的 argparse 没改）。

```python
        tasks.append({
            "name": f"w-{i}",
            "template": "worker",
            "arguments": {"parameters": [
                {"name": "script", "value": shlex.join(command)},
                {"name": "memory", "value": s.memory_limit},
                {"name": "biz-id", "value": s.upload_id[:63]},
            ]},
        })
```

- **98–106 行**：每个 spec 一个 DAG task。三个要点：
  - `"name": f"w-{i}"`：task 名用**下标**而不是业务 id——K8s/Argo 对名字字符集
    有限制（小写字母数字和 `-`），业务 id 不保证合规；下标和 spec 的对应关系
    在 `_collect` 里用同一个 `enumerate` 顺序还原（185 行），两边靠"同序"约定。
  - 所有 task 引用**同一个模板** `worker`，差异全部通过参数注入——CR 体积小，
    模板只写一遍。
  - `shlex.join(command)`：把 list 安全拼成一条 shell 字符串（含空格/特殊字符
    的参数会被正确引号包裹）。为什么要拼成字符串？因为容器的 command 固定为
    `bash -c`，整条命令是模板参数（139–140 行）——这样**一个模板就能跑任意
    入口**（ingest / 训练 / 将来用户自定义），不用为每种 worker 写一个模板。
  - `biz-id` 截断到 63 字符：K8s label 值的长度上限。

```python
    waves = -(-len(specs) // max(parallelism, 1))
```

- **109 行**：向上取整的波数（负负除法技巧，等价 `ceil(len/parallelism)`）。
  例：45 个 spec、parallelism=20 → 3 波。用于算 workflow 级超时（123 行）。

```python
    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {
            "name": name,
            "namespace": settings.k8s_namespace,
            "labels": {"app": WORKER_LABEL, "dagster-run-id": run_id[:63]},
        },
```

- **113–117 行**：元数据。`dagster-run-id` label 是双向导航键——
  从 Dagster run 页拿 run_id → `kubectl get wf -l dagster-run-id=xxx` 找 workflow；
  反过来从 Argo UI 看到 workflow → label 里有 run_id 回 Dagster 查业务上下文。

```python
        "spec": {
            "entrypoint": "main",
            "serviceAccountName": "dagster",
            "parallelism": parallelism,
            "activeDeadlineSeconds": timeout_seconds * waves + 120,
            "volumes": [{"name": "lance", "persistentVolumeClaim": {"claimName": "edp-lance"}}],
```

- **119 行** `entrypoint: main`：从名为 main 的模板开始执行（126 行的 DAG）。
- **120 行** `serviceAccountName: dagster`：workflow 的 pod 用 dagster SA 跑。
  必须显式指定——argoexec sidecar 要用这个 SA 写 `workflowtaskresults`
  （RBAC 见 `30-argo.yaml` 57–60 行），默认 SA 没这个权限节点会报错。
- **121 行** `parallelism`：**批内并发上限，替代旧线程池分波**。Argo controller
  保证任意时刻最多这么多 pod 在跑，其余 task 排队——200 条的批不会一次打爆节点。
- **123 行** workflow 级 `activeDeadlineSeconds = 单 pod 超时 × 波数 + 120s 余量`：
  兜底"整批卡死"，**尤其是 pod 一直 Pending 调度不上的情况**——模板级 deadline
  从 pod 启动才计时，Pending 的 pod 它管不到；workflow 级 deadline 从提交计时，
  到点 controller 强制终止一切。旧形态里这个兜底靠线程 future 的 grace 超时 +
  主动删 pod，现在一个字段解决。
- **124 行**：Lance PVC 声明在 workflow 级，模板里引用（142 行）——worker 写
  样本切片需要挂 `/data/lance`（与 run pod、Ray 共用同一个 PVC）。

```python
            "templates": [
                {"name": "main", "dag": {"tasks": tasks}},
```

- **126 行**：main 模板就是一个 DAG，tasks 之间**没有依赖边**（没写 `dependencies`）
  → 全部可并行，实际并发由 121 行的 parallelism 节流。

```python
                {
                    "name": "worker",
                    "inputs": {"parameters": [{"name": "script"}, {"name": "memory"}, {"name": "biz-id"}]},
                    "activeDeadlineSeconds": timeout_seconds,
                    "metadata": {"labels": {"app": WORKER_LABEL, "biz-id": "{{inputs.parameters.biz-id}}"}},
```

- **128–131 行**：worker 模板头。
  - `inputs.parameters` 声明三个参数，task 那边（101–105 行）逐个传值，
    模板体内用 `{{inputs.parameters.xxx}}` 占位符引用（Argo 提交时渲染）。
  - **130 行** 模板级 `activeDeadlineSeconds`：**单 pod 超时**，与旧形态的
    pod spec 字段等价（kubelet 到点杀容器）。ingest 默认 600s
    （`INGEST_WORKER_TIMEOUT_SECONDS`），训练默认 1800s（`TRAIN_*`）。
  - `biz-id` label 打在 pod 上：`kubectl get pod -l biz-id=upload-xxx` 直接定位
    某个上传对应的 worker pod。

```python
                    "podSpecPatch": '{"containers":[{"name":"main","resources":'
                                    '{"requests":{"cpu":"100m","memory":"256Mi"},'
                                    '"limits":{"memory":"{{inputs.parameters.memory}}"}}}]}',
```

- **133–135 行**：**动态内存限额的关键技巧**。Argo 的模板参数替换**不作用于**
  `container.resources` 结构化字段（那是强类型的 Quantity，占位符字符串塞不进去）；
  官方指定的逃生通道是 `podSpecPatch`——一段 JSON **字符串**（字符串里可以做参数
  替换），Argo 在建 pod 前把它 strategic-merge 进 pod spec。这里 patch 的效果：
  main 容器 requests 固定 100m CPU / 256Mi，**memory limit 用参数注入**——
  同一批里 attempt=1 的 upload 给 1Gi、OOM 重试过的给 2Gi，互不影响。
- 不设 CPU limit：worker 是批处理，允许突发用满节点空闲 CPU，只有内存需要硬限
  （内存超限的后果是 OOMKill，这正是我们要的明确信号）。

```python
                    "container": {
                        "image": settings.edp_image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["bash", "-c"],
                        "args": ["{{inputs.parameters.script}}"],
                        "envFrom": [{"configMapRef": {"name": "edp-env"}}],
                        "volumeMounts": [{"name": "lance", "mountPath": "/data/lance"}],
                    },
```

- **136–143 行**：容器本体。
  - 镜像与 run pod 同源（`edp-env` 里的 `EDP_IMAGE`）；将来用户自定义镜像时，
    改成从 `WorkerSpec` 传 image 即可（结构已留好——参数化一个字段的事）。
  - `bash -c {{script}}`：见上文，一个模板承载任意命令行。
  - `envFrom edp-env`：worker 需要 MinIO endpoint/凭据（读 input.json、写
    manifest/parquet）。**注意 worker 仍然不连 PG、不碰 catalog**——环境变量
    里虽然有那些地址，代码路径上不用。
  - **没有任何 Pipes 环境变量注入**——worker 里的 `_maybe_open_pipes()` 检测
    不到 bootstrap 变量，自动退化为普通进程。这就是"用户镜像零依赖"的落点。

### 1.6 `_submit`（150–159 行）：提交 + 409 幂等

```python
    try:
        api.create_namespaced_custom_object(GROUP, VERSION, settings.k8s_namespace, PLURAL, workflow)
    except ApiException as e:
        if e.status == 409:
            logger.info("workflow %s 已存在，直接跟踪", ...)
        else:
            raise
```

- 409 AlreadyExists = 同名 workflow 已存在。结合 1.4 节的确定性命名，这只可能是
  "同一个 run、同一批"的重复提交（run pod 中途被重启重放、并发触发撞车）。
  处理方式：**不报错、不删旧的，直接进入等待循环跟踪它**——效果上等价于
  "断线重连"。其他状态码（403 RBAC 缺失、404 CRD 没装）原样上抛，由调用方的
  `classify_exception(where="run")` 定码。

### 1.7 `_wait`（162–179 行）：轮询即心跳

```python
    deadline = time.monotonic() + deadline_seconds
    wf: dict = {}
    while time.monotonic() < deadline:
        try:
            heartbeat()
        except Exception:
            logger.exception("saga heartbeat failed")
        try:
            wf = api.get_namespaced_custom_object(...)
            if (wf.get("status") or {}).get("phase") in _TERMINAL_PHASES:
                return wf
        except Exception:
            logger.exception("poll workflow %s failed", name)
        time.sleep(POLL_SECONDS)
    logger.warning("workflow %s 等待超时（%ss），按当前观测收尾", ...)
    return wf
```

- **单线程、无监视线程**——这是对旧 `_WaveMonitor` 的整体替换。每 5s 一个 tick：
  1. **先刷心跳**（168 行）：调用方传进来的闭包（ingest 是
     `batch.advance_many("PARSE", wave)`，训练是 `saga.advance("TRAIN")`），
     刷新 `saga_log.updated_at`，防止 watchdog 把活着的 run 误判为死。
     心跳失败**只记日志不中断**（169–170 行）——PG 抖一下不应该放弃整批等待；
     连续失败超过 takeover 窗口的后果是 watchdog 可能补发触发，但新 run 的
     saga CAS 抢不到锁，依然安全（双保险的意义）。
  2. **再查 workflow**（171–174 行）：只 GET 一个对象；到终态立即返回。
     GET 失败同样只记日志（K8s API 抖动），下一 tick 重试。
- **166 行 / 178–179 行**：本地 deadline 兜底（值 = workflow 级 deadline + 60s，
  见 1.9 节 223 行）。正常情况下 workflow 自己的 deadline 先到、controller 把它
  置为 Failed、循环从 173 行退出；只有 controller 本身挂了才会走到 178 行——
  此时返回**最后一次成功 GET 的对象**（可能是空 dict），调用方按"无清单 +
  节点未观测"处理成 WORKER_LOST，业务照常收尾，不会挂死。

### 1.8 `_collect`（182–200 行）：从 status.nodes 还原每个 spec 的终态

```python
    outcomes = {s.upload_id: PodOutcome(upload_id=s.upload_id, workflow=name) for s in specs}
    by_task = {f"w-{i}": s.upload_id for i, s in enumerate(specs)}
```

- **184 行**：先给每个 spec 建一个空 outcome——**保证返回字典的键永远齐全**。
  哪怕 workflow 对象是空的（等待超时、controller 挂），调用方拿到的也是
  "phase=None 的 outcome"，classify() 给 WORKER_LOST，不会 KeyError。
- **185 行**：用与 `_build_workflow` **相同的 enumerate 顺序**重建 task 名 →
  业务 id 的映射（两个函数都吃同一个 specs list，顺序天然一致）。

```python
    for node in ((wf.get("status") or {}).get("nodes") or {}).values():
        uid = by_task.get(node.get("displayName"))
        if uid is None or node.get("type") != "Pod":
            continue
```

- **186–189 行**：遍历 Argo 记录的所有节点。`status.nodes` 是
  `{nodeId: {...}}` 的字典，里面混着 DAG 节点自身、重试组节点等——
  `type != "Pod"` 的一律跳过；`displayName` 才是我们起的 task 名（`w-0`…），
  对不上映射的（比如 main 节点）也跳过。

```python
        o.phase = node.get("phase")
        o.message = node.get("message")
        o.pod_names.append(node.get("id", ""))
        exit_code = ((node.get("outputs") or {}).get("exitCode"))
        if exit_code is not None:
            try:
                o.exit_code = int(exit_code)
            except ValueError:
                pass
```

- **190–199 行**：抄录节点终态。`node.id` 就是 pod 名（拿去 MinIO 归档路径 /
  Argo UI 找日志）；`outputs.exitCode` 是 Argo 记录的容器退出码（字符串），
  转 int 失败就留 None——classify() 对 None 有兜底。

### 1.9 `launch_wave`（203–226 行）：对外唯一入口

```python
def launch_wave(
    op_context,            # 兼容旧签名；Argo 形态不需要 Dagster 上下文
    specs: list[WorkerSpec],
    *,
    run_id: str,
    timeout_seconds: int,
    heartbeat,
    parallelism: int = 20,
) -> dict[str, PodOutcome]:
```

- **签名与旧模块保持一致**（多了个带默认值的 parallelism）——两个调用方只改
  import 一行。`op_context` 参数保留但不再使用（旧形态靠它开 Pipes 会话）；
  等两条链稳定后可以连同调用方一起删掉这个参数。

```python
    if not specs:
        return {}
    api = _custom_api()
    name = _workflow_name(run_id, specs)
    _submit(api, _build_workflow(name, specs, run_id, timeout_seconds, parallelism))
    waves = -(-len(specs) // max(parallelism, 1))
    wf = _wait(api, name, timeout_seconds * waves + 180, heartbeat)
    phase = (wf.get("status") or {}).get("phase")
    logger.info("workflow %s 终态：%s（Argo UI 可按此名检索节点日志）", name, phase)
    return _collect(wf, specs, name)
```

- **217–218 行**：空批直接返回（防御 ingest 全部被 claim 冲突挤掉的边界）。
- **221 行**：构造 + 提交（幂等，见 1.6）。
- **223 行**：本地等待上限 = `timeout × 波数 + 180s`，比 CR 里的 workflow 级
  deadline（`+120s`，1.5 节 123 行）**多 60s**——保证正常情况下总是 Argo 先
  超时、我们收到的是"Argo 判定的 Failed"而不是自己放弃，节点终态信息更完整。
- **226 行**：无论 workflow 成功失败，都走 `_collect` 逐节点还原——workflow
  级的 Succeeded/Failed 本身不进业务判定（一个节点 OOM 会让 workflow Failed，
  但其他 199 个节点照样是 Succeeded，要逐个看）。

### 1.10 调用方怎么消费返回值（判定责任的划分）

`launch_wave` 只报告**物理事实**，业务判定在调用方（两条链同一段逻辑）：

```text
for 每个业务 id：
    读 staging/{run_id}/{id}/manifest.json
    ├─ 有清单 且 status=ok      → 成功，行进 Iceberg commit
    ├─ 有清单 且 带 error_code  → 业务失败（worker 自报：DATA_PARSE_ERROR 等）
    │                             → saga fail_one 落码，NOT_RETRYABLE 等人工
    └─ 无清单                   → pod 级失败 → outcomes[id].classify()
                                  → WORKER_OOM / WORKER_TIMEOUT / WORKER_LOST
                                  → saga fail_one 落码，watchdog 自动重试+内存升档
```

对应代码：ingest 在 `engines/spark/ingest_append.py` 224–239 行，
训练在 `engines/training/run_job.py` 152–161 行。

---

## 2. `deploy/k8s/30-argo.yaml`（EDP 叠加配置，75 行）

基础安装来自官方清单 `deploy/k8s/argo/namespace-install-v4.0.7.yaml`
（CRD + workflow-controller + argo-server，namespace 级——只监管 data 命名空间，
不需要集群管理员权限）。本文件在其后 apply，叠加三样：

### 2.1 MinIO 凭据 Secret（10–18 行）

```yaml
kind: Secret
metadata:
  name: argo-minio-cred
stringData:
  accesskey: minioadmin
  secretkey: minioadmin123
```

给 argoexec sidecar 上传日志归档用。与 `edp-env` 里的 MinIO 凭据同值，但 Argo
的 artifact 配置只认 Secret 引用格式，所以单独建一份。

### 2.2 workflow-controller-configmap（20–45 行）

```yaml
  artifactRepository: |
    archiveLogs: true
    s3:
      endpoint: minio:9000
      insecure: true
      bucket: lake
      keyFormat: "argo/{{workflow.name}}/{{pod.name}}"
```

- 官方 namespace-install 自带一个**空的**同名 configmap；apply.sh 里本文件在
  其之后 apply，直接覆盖（文件头注释第 20 行专门说明了这个顺序依赖）。
- `archiveLogs: true`：**日志不丢的关键**。每个 worker pod 结束时，argoexec
  把 main 容器的 stdout 传到 `s3://lake/argo/{workflow}/{pod}/main.log`；
  pod 随后被 GC 删掉也不影响——Argo UI 展示节点日志时自动从归档读。
- 回归时实测的归档对象：
  `argo/edp-24f6db7e-87608a/edp-24f6db7e-87608a-worker-2324310180/main.log`。

```yaml
  workflowDefaults: |
    spec:
      ttlStrategy:
        secondsAfterCompletion: 86400
      podGC:
        strategy: OnPodCompletion
```

- 所有 workflow 的默认值（CR 里没写就继承）：完结 1 天后自动删 CR
  （对齐平台的 retention 语义——排查窗口 1 天，之后靠归档日志 + saga_log）；
  pod 完结立即删（现场三件套：归档日志、staging manifest、`saga_log.error_code`，
  不依赖 pod 尸体）。

### 2.3 RBAC（47–74 行）

```yaml
rules:
  - apiGroups: ["argoproj.io"]
    resources: ["workflows"]
    verbs: ["create", "get", "list", "watch", "delete", "patch"]
  - apiGroups: ["argoproj.io"]
    resources: ["workflowtaskresults"]
    verbs: ["create", "patch"]
```

绑到 dagster SA 上，两条规则对应两种身份：

- `workflows` 增删查改：**run pod 是提交方**（`_submit` create、`_wait` get）；
- `workflowtaskresults` 写权限：**worker pod 也用 dagster SA 跑**（CR 里
  `serviceAccountName: dagster`），每个 pod 里的 argoexec sidecar 通过写这个
  资源向 controller 上报节点结果——没有这条，节点会全部报权限错误。

### 2.4 `apply.sh` 的配套改动

```bash
kubectl -n "$NS" apply --server-side -f deploy/k8s/argo/namespace-install-v4.0.7.yaml
kubectl -n "$NS" apply -f deploy/k8s/30-argo.yaml
```

- `--server-side` 是必须的：Argo 的 Workflow CRD 定义极大，客户端 apply 会把
  完整内容塞进 `last-applied-configuration` 注解，超过 K8s 对注解 256KB 的上限
  直接报错；server-side apply 不写这个注解。
- 开发环境还 patch 了 argo-server 用 `--auth-mode=server`（免 token 登 UI），
  端口转发 `svc/argo-server 2746` 即可访问。

---

## 3. 调用方的接缝（改了什么）

### 3.1 两条链共同的改动：一行 import

```python
# 旧
from common.worker_pods import WorkerSpec, launch_wave
# 新
from common.argo_workflows import WorkerSpec, launch_wave
```

`WorkerSpec` 字段、`launch_wave` 签名、`PodOutcome.classify()` 语义全部兼容，
调用点零改动。`common/worker_pods.py` 已删除。

### 3.2 ingest 链（`engines/spark/ingest_append.py::_fan_out_parse`，167–240 行）

调用点逻辑未变，但有一个值得说明的现状：

```python
    for i in range(0, len(upload_ids), max_parallel):     # 189 行：run 侧仍在切波
        wave = upload_ids[i : i + max_parallel]
        ...
        outcomes = launch_wave(op_context, specs, run_id=run_id,
                               timeout_seconds=timeout,
                               heartbeat=lambda ids=list(wave): batch.advance_many("PARSE", ids))
```

- **run 侧的切波循环保留了**（每波 ≤ `INGEST_WORKER_MAX_PARALLEL` 个，一波一个
  workflow）。这在 Argo 形态下是**双保险但冗余**——workflow 自己的 parallelism
  也能限并发。保留它是"简单优先"的取舍：改动最小、行为与回归过的旧形态一致。
  后续清理项：去掉外层循环，整批一个 workflow（200 节点一个 CR 完全没问题），
  让 parallelism 独自负责节流——只影响本函数，saga/manifest 判定不用动。
- heartbeat 闭包只刷本波已 claim 的 id（`batch.advance_many("PARSE", wave)`），
  同时兼做 fencing 检查的写路径（advance 内部校验 owner）。
- **210 行** `memory_limit=_memory_tier(batch.attempts.get(uid, 1), tiers)`：
  内存档位逐 upload 独立——同一波里首跑的 1Gi、OOM 重试的 2Gi 并存，落到
  workflow 里就是不同 task 的 memory 参数不同（podSpecPatch 渲染出不同 limit）。

### 3.3 训练链（`engines/training/run_job.py`，127–161 行）

```python
    # ---- TRAIN：提交 Argo Workflow 拉起训练 worker（fan-out=1），等待并收清单 ----
    saga.advance("TRAIN")
    spec = WorkerSpec(upload_id=job_id, staging_prefix=prefix,
                      memory_limit=tiers[min(attempt, len(tiers)) - 1],
                      command=["python", "-m", "engines.worker.train_mock",
                               "--job-id", job_id, "--run-id", run_id,
                               "--staging-prefix", prefix])
    outcomes = launch_wave(op_context, [spec], run_id=run_id,
                           timeout_seconds=timeout,
                           heartbeat=lambda: saga.advance("TRAIN"))
```

- fan-out=1：一个训练 job = 单节点 workflow。自定义 `command` 走 1.5 节 92 行的
  `s.command or ...` 分支——同一个 worker 模板跑训练入口。
- heartbeat 是 `saga.advance("TRAIN")`（单条 saga，非批量版），每 5s 刷一次。
- 收清单判定（152–161 行）与 ingest 同构：无清单 → `outcomes[job_id].classify()`；
  有清单带码 → worker 自报；都走 `_fail`（saga fail + platform_job failed + alert）
  后抛 `TrainingFailed`，由 watchdog 按码决定重试。

### 3.4 worker 侧（零改动）

`engines/worker/ingest_parse.py` / `train_mock.py` 的代码没动：
- 入口 argparse 参数一致（命令行由 CR 渲染，同旧形态）；
- `_maybe_open_pipes()` 检测不到 Pipes bootstrap 环境变量 → 自动退化为普通进程
  （这个兼容分支保留着，将来如果想把日志实时流回 Dagster UI，加环境变量注入 +
  S3 message reader 即可，worker 不用改）；
- manifest.json / 厚表 parquet 的 staging 契约原样。

---

## 4. 回归验证记录（2026-07-17，minikube）

| # | 场景 | 结果 |
|---|------|------|
| 1 | 冒烟：单节点 workflow（echo + 环境变量 + PVC 挂载检查） | workflow Succeeded，节点 Succeeded |
| 2 | 训练成功路径：`POST /train`（grasp-cls @ v20260717085719） | workflow `edp-cdc9c736-f27457` Succeeded；`platform_job` → done；MLflow run id 落 result |
| 3 | 训练失败路径：`POST /train` 指向不存在的 dataset_version | job → failed，`saga_log.error_code=DATA_EMPTY`（run 侧 PREPARE 阶段自报，未起 workflow——符合预期）|
| 4 | ingest e2e：r-002 上传 2 个 episode → kafka sensor → run | workflow `edp-24f6db7e-87608a` Succeeded；upload_session → done；下游资产照常物化 |
| 5 | retry API：对 failed job 重试 / 对 done job 重试 | 前者 200（reset 回 ready + previous_error 带出），后者 409 |
| 6 | 日志归档：pod 删除后检索 MinIO | `argo/{workflow}/{pod}/main.log` 三个对象在，内容为 worker stdout |

已知的后续清理项（不影响当前正确性）：

1. ingest 的外层切波循环可去掉，改为整批一个 workflow（见 3.2）；
2. `launch_wave` 的 `op_context` 参数已无用途，可连调用方一起删；
3. 批规模若未来超过千级，`_build_workflow` 改用 `withParam` + Argo 节点状态
   卸载（nodeStatusOffload），CR 体积与 etcd 上限问题见 README 3.6.3 讨论；
4. 若需要 worker 日志实时出现在 Dagster UI：对自有 worker 注入 Pipes 环境变量 +
   run 侧挂 S3 message reader（用户镜像仍零依赖），属可选体验优化。
