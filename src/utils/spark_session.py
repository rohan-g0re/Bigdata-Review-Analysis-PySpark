from .spark_config import configure_spark

def get_spark_session(app_name="SteamReviewsAnalysis"):
    """Get or create a Spark session with proper configuration."""
    return configure_spark() 