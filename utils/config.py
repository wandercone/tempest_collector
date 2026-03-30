import os
import sys

def require_env(vars_list):
    """
    Check that each variable in vars_list exists in the environment.
    
    Args:
        vars_list (list): List of environment variable names to check.
    
    Returns:
        dict: Dictionary mapping variable names to their values.
        Exits with error code 1 if any variables are missing.
    """
    missing = [v for v in vars_list if not os.getenv(v)]
    if missing:
        print(f"Error: missing required environment variables: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return {v: os.getenv(v) for v in vars_list}