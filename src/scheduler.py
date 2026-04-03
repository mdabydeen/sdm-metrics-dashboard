"""APScheduler-based ingestion orchestrator."""

import logging
import logging.config
from pathlib import Path
from datetime import datetime
import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.config import get_config
from src.ingestion.jira import JiraSprintIngestor, JiraIssueIngestor, JiraEpicIngestor
from src.ingestion.github import GithubPRIngestor, GithubDeploymentIngestor
from src.metrics.compute import compute_metrics


# Configure logging
logging_config_path = Path("config/logging.yaml")
if logging_config_path.exists():
    with open(logging_config_path) as f:
        config_dict = yaml.safe_load(f)
        logging.config.dictConfig(config_dict)
else:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def init_scheduler():
    """Initialize and configure the scheduler."""
    scheduler = BlockingScheduler()
    config = get_config()

    # JIRA ingestors - 15 minute interval, first run immediately
    scheduler.add_job(
        lambda: JiraSprintIngestor(config).run(),
        IntervalTrigger(minutes=15),
        id="jira_sprints",
        name="JIRA Sprints",
        next_run_time=datetime.now(),
    )

    scheduler.add_job(
        lambda: JiraIssueIngestor(config).run(),
        IntervalTrigger(minutes=15),
        id="jira_issues",
        name="JIRA Issues",
        next_run_time=datetime.now(),
    )

    scheduler.add_job(
        lambda: JiraEpicIngestor(config).run(),
        IntervalTrigger(minutes=30),
        id="jira_epics",
        name="JIRA Epics",
        next_run_time=datetime.now(),
    )

    # GitHub ingestors - 15 minute interval, first run immediately
    scheduler.add_job(
        lambda: GithubPRIngestor(config).run(),
        IntervalTrigger(minutes=15),
        id="github_prs",
        name="GitHub Pull Requests",
        next_run_time=datetime.now(),
    )

    scheduler.add_job(
        lambda: GithubDeploymentIngestor(config).run(),
        IntervalTrigger(minutes=30),
        id="github_deployments",
        name="GitHub Deployments",
        next_run_time=datetime.now(),
    )

    # Metrics computation - run after JIRA ingestors complete (every 20 minutes)
    scheduler.add_job(
        compute_metrics,
        IntervalTrigger(minutes=20),
        id="compute_metrics",
        name="Compute Sprint Metrics",
        next_run_time=datetime.now(),
    )

    return scheduler


def main():
    """Start the scheduler."""
    logger.info("Starting SDM Metrics Dashboard scheduler...")
    scheduler = init_scheduler()
    logger.info("Scheduler initialized with jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name} (ID: {job.id})")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
