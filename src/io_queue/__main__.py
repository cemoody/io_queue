"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """IOQueue."""


if __name__ == "__main__":
    main(prog_name="io_queue")  # pragma: no cover
