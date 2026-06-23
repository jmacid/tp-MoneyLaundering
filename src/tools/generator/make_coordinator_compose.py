import argparse
from pathlib import Path


def make_coordinator_compose(coordinators_count: int, output_path: str = "docker-compose.yml") -> None:
    if coordinators_count < 1:
        raise ValueError("coordinators_count must be at least 1")

    compose = {
        "services": {
            "rabbitmq": {
                "image": "rabbitmq:3-management",
                "container_name": "rabbitmq",
                "ports": ["5672:5672", "15672:15672"],
                "environment": {
                    "RABBITMQ_DEFAULT_USER": "guest",
                    "RABBITMQ_DEFAULT_PASS": "guest",
                },
                "healthcheck": {
                    "test": ["CMD", "rabbitmq-diagnostics", "-q", "check_port_connectivity"],
                    "interval": "5s",
                    "timeout": "5s",
                    "retries": 10,
                },
                "networks": ["money_laundering_net"],
            }
        },
        "volumes": {
            "coordinator_data": None,
        },
        "networks": {
            "money_laundering_net": {
                "external": True,
            }
        },
    }

    for coordinator_id in range(1, coordinators_count + 1):
        service_name = "coordinator" if coordinator_id == 1 else f"coordinator_{coordinator_id}"

        compose["services"][service_name] = {
            "build": {
                "context": "../..",
                "dockerfile": "coordinator/base/Dockerfile",
            },
            "container_name": service_name,
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy",
                }
            },
            "environment": {
                "PYTHONPATH": "/app",
                "RABBITMQ_HOST": "rabbitmq",
                "COORDINATOR_QUEUE": "coordinator_control_queue",
                "REPORT_RETRY_SECONDS": 5,
                "NODE_TIMEOUT_SECONDS": 9999,
                "MONITOR_INTERVAL_SECONDS": 9999,
                "STATE_DB_PATH": "/app/data/state.db",
                "COORDINATOR_ID": coordinator_id,
            },
            "networks": ["money_laundering_net"],
            "volumes": ["coordinator_data:/app/data"],
            "restart": "unless-stopped",
        }

    yaml_content = dump_yaml(compose)
    Path(output_path).write_text(yaml_content, encoding="utf-8")

    print(f"Docker compose generated at: {output_path}")
    print(f"Coordinators: {coordinators_count}")


def dump_yaml(data: dict) -> str:
    def format_value(value):
        if value is True:
            return "true"
        if value is False:
            return "false"
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return str(value)

    def render(obj, indent=0):
        lines = []
        spaces = " " * indent

        if isinstance(obj, dict):
            for key, value in obj.items():
                if value is None:
                    lines.append(f"{spaces}{key}:")
                elif isinstance(value, (dict, list)):
                    lines.append(f"{spaces}{key}:")
                    lines.extend(render(value, indent + 2))
                else:
                    lines.append(f"{spaces}{key}: {format_value(value)}")

        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    lines.append(f"{spaces}-")
                    lines.extend(render(item, indent + 2))
                elif isinstance(item, list):
                    inline = ", ".join(f'"{x}"' if isinstance(x, str) else str(x) for x in item)
                    lines.append(f"{spaces}- [{inline}]")
                else:
                    lines.append(f"{spaces}- {format_value(item)}")

        return lines

    return "\n".join(render(data)) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a docker-compose file with N coordinator services."
    )

    parser.add_argument(
        "--coordinators",
        "-c",
        type=int,
        required=True,
        help="Number of coordinator services to generate.",
    )

    parser.add_argument(
        "--output",
        "-o",
        default="docker-compose.yml",
        help="Output docker-compose file path. Default: docker-compose.yml",
    )

    args = parser.parse_args()

    make_coordinator_compose(
        coordinators_count=args.coordinators,
        output_path=args.output,
    )

# python3 make_coordinator_compose.py -c 3 -o docker-compose.coordinators.yml
# docker compose -f docker-compose.coordinators.yml up --build
# docker compose -f docker-compose.coordinators.yml down -v
if __name__ == "__main__":
    main()