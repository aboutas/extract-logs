from typing import Dict, List, Tuple, Any
import re

class ExecutionSummaryParser:
    """
    Parses the stdout logs from Beeline, a CLI tool for Hive SQL Engine queries.

    This class processes the  unstructured stdout logs from Beeline, which
    are generated during the execution of SQL queries. These logs include detailed
    execution information and metrics, appearing in no specific line in the log file.

    The goal is to identify, extract, and organize these execution metrics into
    Python dictionaries, maintaining the integrity of the original metric names
    (e.g., 'REDUCE_INPUT_RECORDS') and their corresponding values (e.g., 1200). The
    organized data facilitates further analysis, reporting, or integration with
    other systems.

    Key functionalities:
    - Parsing of unstructured Beeline log files to identify execution metrics.
    - Extraction and organization of metrics into structured Python dictionaries for
      easy access and analysis.
    
    There are 3 main functions in this class that correspond to the assignment's 3 tasks:
    
        
    1. parse_query_execution_summary(lines: List[str]) -> Dict[str, float]:
        Parses the query execution summary from given lines of log,  identifying the
        start and end of the section and various stages of the parsing process through 
        flags and extracting operation names and durations along with their units. 
        This was made in an agnostic manner towards the operation names. But also, 
        the unit of the duration was extracted in order to be saved in the operation 
        name although it wasn't mentioned.
        
        Example of parsing wasnt in the description of the task.
    
    2. parse_task_execution_summary(lines: List[str]) -> Dict[str, float]:
        Parses the task execution summary from given lines of log, identifying the
        start and end of the section and extracting task metrics such as duration,
        CPU time, GC time, and records processed, while maintaining the upper key name.
        In this case, the task names were considered as known. An advancement could be 
        the name agnostic conversion of this.
        
        Example of parsing:
        {
        "Map 1" : { "DURATION(ms)" : 65013.00 ,   "CPU_TIME(ms)": 516890  , 
        "GC_TIME(ms)" : 7624  ,   "INPUT_RECORDS" : 13119189  ,"OUTPUT_RECORDS" : 1200} ,
        }
        
        
    
    3. parse_detailed_metrics(lines: List[str]) -> Dict[str, Dict[str, float]]:
        Parses detailed metrics from given lines of log, identifying start and end of section,
        categorizing metrics under their respective groups and parsing numerical values.
        
        Example of parsing:
        {
        "org.apache.tez.common.counters.DAGCounter" : {"NUM_SUCCEEDED_TASKS": 58 , "TOTAL_LAUNCHED_TASKS" : 58 , ...} ,
        "File System Counters:" : { ... } ,

    """

    def __init__(self, verbose: int = 0):
        """
        Initializes the parser with a verbosity level.
        
        Args:
            verbose (int): The verbosity level for logging output ->[0,1].
        """
        self.verbose = verbose

    def log(self, message: str) -> None:
        """
        Logs a message to the console, depending on the verbosity level.
        
        Args:
            message (str): The message to log.
        """
        if self.verbose == 1:
            print(message)

    def parse_duration_and_units(self, duration_str: str) -> Tuple[str, str]:
        """
        This was an extra to the description, since it was observed that the value was
        carrying an extra letter for unit, we pass that unit into the operation name and
        keep the value as the duration.
        Parses the duration and its units from a string, separating the numerical
        part from the unit part using a regular expression.
        
        Args:
            duration_str (str): The string containing the duration and possibly units.
        
        Returns:
            Tuple[str, str]: The duration and its units.
        """
        match = re.search(r"([0-9.]+)([a-zA-Z]*)", duration_str)
        if match:
            return match.group(1), match.group(2)  # duration, units
        else:
            return duration_str, ""  # Default to no units if parsing fails

    def parse_query_execution_summary(self, lines: List[str]) -> Dict[str, float]:
        """
        Parses the query execution summary from given lines of log, identifying
        various stages of the parsing process through flags and extracting
        operation names and durations along with their units.
        
        Args:
            lines (List[str]): The lines of the log file to be parsed.
        
        Returns:
            Dict[str, float]: A dictionary containing operation names and their durations.
        """
        summary = {}
        found_section_heading = found_first_dash_line = False
        found_operation_duration_heading = found_second_dash_line = False

        for index, line in enumerate(lines):
            normalized_line = line.strip()

            # Detects different sections and marks of the summary using flags
            if not found_section_heading and "Query Execution Summary" in normalized_line:
                found_section_heading = True
                self.log(f"Found section heading at line {index}.")
                continue

            if found_section_heading and not found_first_dash_line and "----" in normalized_line:
                found_first_dash_line = True
                self.log(f"Found first dash line after section heading at line {index}.")
                continue

            if found_first_dash_line and not found_operation_duration_heading and "OPERATION" in normalized_line and "DURATION" in normalized_line:
                found_operation_duration_heading = True
                self.log(f"Found 'OPERATION DURATION' heading at line {index}.")
                continue

            if found_operation_duration_heading and not found_second_dash_line and "----" in normalized_line:
                found_second_dash_line = True
                self.log(f"Found second dash line, indicating start of operations data at line {index}.")
                continue

            # Parses operation names and durations when in the correct section
            if found_second_dash_line:
                if "----" in normalized_line:
                    self.log(f"Found closing dash line, indicating end of operations data at line {index}.")
                    break

                if "INFO  :" in normalized_line:
                    operation_info = line.split("INFO  :", 1)[1].strip()
                    parts = operation_info.rsplit(" ", 1)
                    if len(parts) == 2:
                        operation_name, duration_str = parts
                        duration, units = self.parse_duration_and_units(duration_str)
                        summary[f"{operation_name.strip()} ({units})"] = float(duration)
                        self.log(f"Operation '{operation_name.strip()} ({units})' with duration '{duration}' parsed at line {index}.")

        return summary

    def parse_task_execution_summary(self, lines: List[str]) -> Dict[str, float]:
        """
        Parses the task execution summary from given lines of log, identifying the
        start and end of the section and extracting task metrics such as duration,
        CPU time, GC time, and records processed.
        
        Args:
            lines (List[str]): The lines of the log file to be parsed.
        
        Returns:
            Dict[str, float]: A dictionary with task names as keys and their metrics as values.
        """
        tasks_summary = {}
        in_section = metrics_started = False

        for index, line in enumerate(lines):
            normalized_line = line.strip()

            if "Task Execution Summary" in normalized_line:
                in_section = True
                self.log(f"Task Execution Summary section start found at line {index}.")
                continue

            if in_section and "VERTICES" in normalized_line and "DURATION(ms)" in normalized_line:
                metrics_started = True
                self.log(f"Metrics header found at line {index}.")
                continue

            # Parses task metrics after the metrics header is found
            if metrics_started and "----" in normalized_line:
                if tasks_summary:  # Indicates the end of the section if tasks_summary is not empty
                    self.log(f"End of Task Execution Summary section found at line {index}.")
                    break

            if metrics_started and "INFO  :" in normalized_line:
                parts = normalized_line.split("INFO  :")[1].split()
                if len(parts) >= 6:
                    vertex_name = " ".join(parts[0:2])
                    metrics = {
                        "DURATION(ms)": float(parts[2].replace(',', '')),
                        "CPU_TIME(ms)": float(parts[3].replace(',', '')),
                        "GC_TIME(ms)": float(parts[4].replace(',', '')),
                        "INPUT_RECORDS": int(parts[5].replace(',', '').split('.')[0]),
                        "OUTPUT_RECORDS": int(parts[6].replace(',', '').split('.')[0])
                    }
                    tasks_summary[vertex_name] = metrics
                    self.log(f"Vertex '{vertex_name}' metrics parsed at line {index}.")

        return tasks_summary

    def parse_detailed_metrics(self, lines: List[str]) -> Dict[str, Dict[str, float]]:
        """
        Parses detailed metrics from given lines of log, categorizing metrics under
        their respective groups and parsing numerical values as int or float where applicable.
        
        Args:
            lines (List[str]): The lines of the log file to be parsed.
        
        Returns:
            Dict[str, Dict[str, float]]: A dictionary of metric groups with their respective metrics and values.
        """
        metrics = {}
        current_group = None
        parsing_enabled = False

        for index, line in enumerate(lines):
            normalized_line = line.strip()

            # Enables parsing upon finding the metric groups' start
            if "org.apache.tez.common.counters.DAGCounter" in line:
                parsing_enabled = True
                self.log(f"Starting parsing at line {index}.")
            
            if parsing_enabled:
                # Detects first-level key for metric groups
                if "INFO  :" in normalized_line and ':' in normalized_line and not normalized_line.startswith("INFO  :    "):
                    group_name = normalized_line.split("INFO  :", 1)[1].split(":", 1)[0].strip()
                    current_group = group_name
                    metrics[current_group] = {}
                    self.log(f"New group '{current_group}' found at line {index}.")

                # Parses second-level key and value if within a group
                elif current_group and normalized_line.startswith("INFO  :    "):
                    parts = normalized_line.split("INFO  :", 1)[1].strip().split(":", 1)
                    if len(parts) == 2:
                        metric_name, metric_value = parts[0].strip(), parts[1].strip().split()[0].replace(',', '')
                        try:
                            metric_value = int(metric_value)
                        except ValueError:
                            try:
                                metric_value = float(metric_value)
                            except ValueError:
                                pass  # Keep as string if conversion fails
                        metrics[current_group][metric_name] = metric_value
                        self.log(f"Metric '{metric_name}' with value '{metric_value}' parsed under group '{current_group}' at line {index}.")

                # Detects the end of the section and concludes parsing
                if "Completed executing command" in normalized_line:
                    next_line_index = index + 1
                    if next_line_index < len(lines) and "OK" in lines[next_line_index]:
                        self.log(f"End of section detected at line {index}. Parsing completed.")
                        del metrics[current_group]  # Remove the last group as it's the exit indicator
                        break

        return metrics