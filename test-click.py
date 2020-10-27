import click


@click.group()
@click.version_option()
def cli():
    """Tanit.
    tanit: A simple thrift client for tanit service.
    """


@cli.command()
#@click.option("--standalone", "-s", is_flag=True, help="Enables standalone mode.")
def master():
    """Run the tanit master."""
    print("Start master")


@cli.command()
def worker():
    """Run the tanit worker."""
    print("Start worker")

@cli.group("jobs")
def jobs():
    """Manage tanit jobs."""

@jobs.command("-submit")
@click.argument("job")
def job_submit(job):
    """Submit a job"""

@jobs.command("-list")
def job_list():
    print("list job")

@jobs.command("-status")
def job_status():
    print("list job")

@cli.command()
def admin():
    """Run the tanit admin client."""
    print("Start worker")

if __name__ == "__main__":
    cli()
