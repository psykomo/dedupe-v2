import typer
import yaml
from dedupe_gemini.seeder import seed_command
from dedupe_gemini.config import load_config
from dedupe_gemini.eda import app as eda_app
from dedupe_gemini.etl import app as etl_app
from dedupe_gemini.deduplication import app as dedupe_app
from dedupe_gemini.check import app as check_app

app = typer.Typer()


@app.command()
def hello(name: str):
    """
    Say hello to someone.
    """
    print(f"Hello {name}")


@app.command()
def goodbye(name: str, formal: bool = False):
    """
    Say goodbye to someone.
    """
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")


@app.command()
def seed(
    count: int = typer.Option(1000, help="Number of records to seed"),
    duplicates: float = typer.Option(None, help="Percentage of duplicates (0.0 to 1.0)"),
    batch_size: int = typer.Option(None, help="Batch size for insertion"),
):
    """
    Seed the database with synthetic data.
    """
    # Load defaults from config
    config = load_config()
    
    # Use config values if not overridden by command line args
    if duplicates is None:
        duplicates = config.get("seeding", {}).get("default_duplicates", 0.05)
        
    if batch_size is None:
        batch_size = config.get("seeding", {}).get("default_batch_size", 1000)
    
    seed_command(count=count, duplicates=duplicates, batch_size=batch_size)


@app.command()
def config():
    """
    Display the current configuration.
    """
    cfg = load_config()
    print(yaml.dump(cfg, default_flow_style=False))

app.add_typer(eda_app, name="eda")
app.add_typer(etl_app, name="etl")
app.add_typer(dedupe_app, name="deduplicate")
app.add_typer(check_app, name="check")

if __name__ == "__main__":
    app()
