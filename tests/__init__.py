# tests/__init__.py
import sys
import os

# Add the project root to sys.path to allow imports of 'config' and other modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)
