"""
Honey Extract: A script to extract and parse execution summaries from Beeline logs.

Version: v.1.0
Author: Thanasis Boutas

Description:
This script reads a Beeline log file, extracts the execution summaries, and saves them as JSON files.

1. The configuration settings are loaded from a 'config.json' file in the same directory as the script.
2. Beeline log file must be saved in the path indicated in the config.log
3. The parsed summaries are saved in a directory specified in the configuration settings.
4. The configuration file should contain the following settings:
    - log_file_path: The path to the Beeline log file to be parsed.
    - save_path: The directory where the parsed summaries will be saved.
    - verbosity: The verbosity level for the parser (0 for no verbosity, 1 for verbosity)
5. To work properly in the same directory as the script the execution_summary_parser.py must be include

"""

# Basic Imports
import json
import os
import logging

# Propriatery Imports
from execution_summary_parser import ExecutionSummaryParser

def load_config(config_path: str = "config.json") -> dict:
    """
    Loads configuration settings from a JSON file.
    
    Parameters:
        config_path (str): The file path to the configuration JSON file. Defaults to 'config.json'.
    
    Returns:
        dict: The configuration settings loaded from the JSON file.
    """
    with open(config_path, "r") as config_file:
        return json.load(config_file)

def ensure_directory_exists(path: str) -> None:
    """
    Ensures that the specified directory exists, creating it if necessary.
    
    Parameters:
        path (str): The path to the directory to check and potentially create.
    """
    os.makedirs(path, exist_ok=True)

def save_to_file(filename: str, data: dict, save_path: str) -> None:
    """
    Saves the given data to a JSON file at the specified path.
    
    Parameters:
        filename (str): The name of the file to save the data in.
        data (dict): The data to be saved.
        save_path (str): The directory path where the file will be saved.
    """
    with open(os.path.join(save_path, filename), "w") as file:
        json.dump(data, file, indent=4)

def parse_log_data(parser: ExecutionSummaryParser, lines: list) -> dict:
    """
    Parses the log data and returns the summaries in a dictionary.
    
    Parameters:
        parser (ExecutionSummaryParser): The parser instance used to parse the log lines.
        lines (list): The lines of the log file to be parsed.
    
    Returns:
        dict: A dictionary containing the parsed summaries.
    """
    summaries = {
        "query_execution_summary": parser.parse_query_execution_summary(lines),
        "task_execution_summary": parser.parse_task_execution_summary(lines),
        "detailed_metrics_summary": parser.parse_detailed_metrics(lines)
    }
    return summaries

def save_parsed_data(summaries: dict, save_path: str) -> None:
    """
    Saves the parsed summaries into separate JSON files.
    
    Parameters:
        summaries (dict): The parsed summaries to be saved.
        save_path (str): The directory path where the files will be saved.
    """
    for key, data in summaries.items():
        save_to_file(f"{key}.json", data, save_path)

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == "__main__":
    try:
        # Load the configuration settings from a file
        config = load_config()
        
        # Extract the verbosity, log file path, and save path from the config
        verbosity = config.get("verbosity", 0)
        log_file_path = config["log_file_path"]
        save_path = config["save_path"]
        
        # Ensure the directory where parsed data will be saved exists
        ensure_directory_exists(save_path)
        
        # Initialize the parser with the configured verbosity level
        parser = ExecutionSummaryParser(verbose=verbosity)
        
        # Read the lines from the log file specified in the configuration
        with open(log_file_path, "r") as file:
            lines = file.readlines()
        
        # Parse the log data and get the parsed summaries
        parsed_summaries = parse_log_data(parser, lines)
        
        # Save the parsed summaries to files in the specified save path
        save_parsed_data(parsed_summaries, save_path)
        
        # Log a message indicating successful completion
        logging.info("Parsed summaries have been saved successfully.")
    except Exception as e:
        # Log any errors that occur during the script execution
        logging.error(f"Failed to execute script: {e}")