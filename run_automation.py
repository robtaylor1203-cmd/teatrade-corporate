import subprocess
import logging
# Use the standard datetime library
import datetime
import os
import sys

# Configuration
# IMPORTANT: Ensure this path is correct for your local setup.
REPO_PATH = r"C:\Users\mikin\projects\NewTeaTrade"

# Determine the Python executable path (e.g., venv)
PYTHON_EXECUTABLE = sys.executable 

# Define the jobs to run sequentially
JOBS_TO_RUN = [
    {"name": "Mombasa Processor (ETL)", "script": "process_mombasa_data.py"},
    {"name": "Mombasa Analyzer (JSON Generation)", "script": "analyze_mombasa.py"},
    # V6 FIX: Added build_library.py to ensure the main library JSON is updated after analysis.
    {"name": "Build Library (Consolidation)", "script": "build_library.py"},
]

# Files/Directories to commit automatically
FILES_TO_COMMIT = [
    "market_reports.db",
    "market-reports-library.json", # The consolidated library file
    "report_data/" # Commit the entire data directory
]

# Set up logging to stdout
logging.basicConfig(level=logging.INFO, format='AUTOMATION: %(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

# Import GitPython if available
try:
    import git
    GIT_AVAILABLE = True
except ImportError:
    GIT_AVAILABLE = False
    logging.warning("GitPython not found (pip install GitPython). Automatic Git synchronization will be disabled.")


def run_script(script_name):
    """Executes a Python script."""
    script_path = os.path.join(REPO_PATH, script_name)
    
    if not os.path.exists(script_path):
        logging.error(f"Script not found at path: {script_path}")
        return False

    logging.info(f"--- Running {script_name} ---")
    try:
        # Execute the script within the repository directory
        result = subprocess.run(
            [PYTHON_EXECUTABLE, script_path],
            capture_output=True,
            text=True,
            check=True,
            cwd=REPO_PATH
        )
        if result.stdout:
            # Print stdout directly so the logs capture the output from the child scripts
            print(result.stdout.strip())
        logging.info(f"--- Finished {script_name} ---")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script_name}: Exit Code {e.returncode}")
        # Log stdout and stderr if the script failed
        if e.stdout:
            logging.error(f"Stdout:\n{e.stdout.strip()}")
        if e.stderr:
            # Decode stderr if it's bytes, otherwise use as is
            stderr_output = e.stderr.decode('utf-8') if isinstance(e.stderr, bytes) else e.stderr
            logging.error(f"Stderr:\n{stderr_output.strip()}")
        return False
    except TypeError as e:
        logging.error(f"An error occurred launching the script. Ensure PYTHON_EXECUTABLE is valid: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while running {script_name}: {e}")
        return False

def git_sync_repository():
    """Implements the Commit-Pull-Push strategy for robust automation."""
    if not GIT_AVAILABLE:
        logging.info("--- Skipping Git Operations (GitPython not available) ---")
        return
        
    logging.info("--- Starting Git Operations (Commit-Pull-Push Strategy) ---")
    try:
        try:
            repo = git.Repo(REPO_PATH)
        except git.exc.InvalidGitRepositoryError:
            logging.error(f"The directory is not a valid Git repository: {REPO_PATH}. Skipping Git sync.")
            return

        try:
            origin = repo.remote(name='origin')
        except ValueError:
            logging.error("Remote 'origin' not found. Skipping Git sync.")
            return
        
        # 1. Add files/directories
        logging.info("Staging changes...")
        # Use repo.git.add() for robust handling of directories/new files
        repo.git.add(FILES_TO_COMMIT)
        
        # 2. Check if there are changes staged (comparing index to HEAD)
        if repo.index.diff('HEAD'):
            # 3. Commit locally
            commit_message = f"Automated data update: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            repo.index.commit(commit_message)
            logging.info(f"Committed changes locally.")

            # 4. Pull latest changes from remote (Rebase)
            logging.info("Pulling latest changes from GitHub (using rebase)...")
            try:
                origin.pull(rebase=True)
                logging.info("Synchronization (rebase) successful.")
            except git.exc.GitCommandError as e:
                logging.error("Rebase failed. Manual intervention might be required.")
                raise e # Re-raise the exception to stop the process here

            # 5. Push the combined history
            logging.info("Pushing changes to remote repository...")
            push_info_list = origin.push()
            
            # 6. Verify Push Results
            push_failed = False
            if push_info_list:
                push_info = push_info_list[0]
                if push_info.flags & (git.PushInfo.ERROR | git.PushInfo.REJECTED):
                    logging.error(f"Push failed or rejected: {push_info.summary}")
                    push_failed = True
            
            if not push_failed:
                logging.info("Push successful.")

        else:
            logging.info("No local changes detected by the automation scripts. Pulling remote updates only.")
            try:
                origin.pull(rebase=True)
                logging.info("Repository is up to date.")
            except git.exc.GitCommandError as e:
                 logging.warning(f"Pull failed (this might happen if the local branch has diverged or authentication failed): {e}")


    except git.exc.GitCommandError as e:
        logging.error(f"Git command error: {e}")
        # Log the specific stderr output from the git command if available
        if hasattr(e, 'stderr'):
            error_msg = e.stderr.decode('utf-8') if isinstance(e.stderr, bytes) else str(e.stderr)
            logging.error(f"Git Stderr: {error_msg}")
        logging.error("Ensure Git is installed, the repository is configured correctly, and authentication is set up.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during Git operations: {e}")

def main():
    logging.info("=== Starting Automated Market Data Pipeline ===")
    
    # Change working directory to the repository path
    if not os.path.exists(REPO_PATH):
        logging.error(f"Repository path not found: {REPO_PATH}")
        logging.error("Please update the REPO_PATH variable in run_automation.py.")
        exit(1)

    try:
        os.chdir(REPO_PATH)
        logging.info(f"Operating in directory: {REPO_PATH}")
    except Exception as e:
        logging.error(f"Failed to change directory to {REPO_PATH}: {e}")
        exit(1)
    
    all_jobs_successful = True
    for job in JOBS_TO_RUN:
        if not run_script(job['script']):
            logging.error(f"{job['name']} failed. Aborting pipeline.")
            all_jobs_successful = False
            break
    
    # Git sync runs if jobs were successful.
    if all_jobs_successful:
         # Set this to False if you do not want automatic Git synchronization
         ENABLE_GIT_SYNC = True 
         if ENABLE_GIT_SYNC:
            git_sync_repository()
    else:
         logging.error("One or more jobs failed. Skipping Git sync to avoid committing partial or failed updates.")

    logging.info("=== Automated Pipeline Finished ===")

if __name__ == "__main__":
    main()