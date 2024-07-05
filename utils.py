import os 
import sys

# Add the path to project_root to sys.path
current_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)


from fetch_weather import fetch_data, transform_data, load_data_to_db