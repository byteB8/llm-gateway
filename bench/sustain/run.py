"""Model server entry point for the GPU server: vLLM's OpenAI-compatible server for one entry of servers.toml.
    python run.py serve gpu0        (started by serve.sh)
The model and its options come from servers.toml, and every process title vLLM would set (APIServer,
EngineCore, workers) is replaced by PROC_TITLE, so on a shared machine ps and nvidia-smi show only
`python run.py`.
"""
import os
import sys


def _pin_process_title() -> None:
    title = os.environ.get("PROC_TITLE", "python run.py")
    try:
        import setproctitle
    except ImportError:
        return
    original = setproctitle.setproctitle
    setproctitle.setproctitle = lambda *_args, **_kwargs: original(title)
    original(title)


# Top level on purpose: spawned child processes re-import this module (as __mp_main__) before vLLM
# retitles them, so the patch is in place in the engine core too.
_pin_process_title()


def serve(name: str) -> None:
    import tomllib

    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "servers.toml"), "rb") as f:
        cfg = tomllib.load(f)
    srv = cfg["servers"][name]
    os.environ["CUDA_VISIBLE_DEVICES"] = str(srv["gpu"])
    sys.argv = ["vllm", "serve", cfg["model"], *cfg["args"], "--host", "127.0.0.1", "--port", str(srv["port"])]
    from vllm.entrypoints.cli.main import main as cli

    cli()


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "serve":
        sys.exit("usage: python run.py serve <server name from servers.toml>")
    serve(sys.argv[2])
