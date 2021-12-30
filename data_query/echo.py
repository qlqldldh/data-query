import typer


def err_echo(e: str) -> None:
    typer.echo(typer.style(e, fg=typer.colors.RED))


def success_echo(s: str) -> None:
    typer.echo(typer.style(s, fg=typer.colors.GREEN))
