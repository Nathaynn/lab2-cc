import argparse
import json
import os
import sys

from runner import run_schedule

DEFAULT_INITIAL_STATE = {
    "A": 100,
    "B": 100,
    "X": 0,
    "Y": 0,
}


def main():
    parser = argparse.ArgumentParser(
        description="Run a transaction schedule under 2PL or MVCC."
    )
    parser.add_argument(
        "--cc",
        choices=["2pl", "mvcc"],
        required=True,
        help="Concurrency control: 2pl (Strict 2PL) or mvcc (Snapshot Isolation)",
    )
    parser.add_argument(
        "--schedule",
        required=True,
        help="Path to schedule file (JSONL)",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output directory for trace.jsonl and final_state.json",
    )
    parser.add_argument(
        "--initial",
        default=None,
        help="Path to initial state JSON (default: built-in A=100,B=100,X=0,Y=0)",
    )
    args = parser.parse_args()

    if args.initial:
        with open(args.initial) as f:
            initial_state = json.load(f)
    else:
        initial_state = dict(DEFAULT_INITIAL_STATE)

    os.makedirs(args.out, exist_ok=True)
    trace_path = os.path.join(args.out, "trace.jsonl")
    state_path = os.path.join(args.out, "final_state.json")

    try:
        trace_events, final_state = run_schedule(
            schedule_path=args.schedule,
            cc=args.cc,
            initial_state=initial_state,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        raise

    
    with open(trace_path, "w") as f:
        for ev in trace_events:
            f.write(json.dumps(ev) + "\n")

    with open(state_path, "w") as f:
        json.dump(final_state, f, indent=2)

    print(f"Trace written to {trace_path}")
    print(f"Final state written to {state_path}")


if __name__ == "__main__":
    main()
