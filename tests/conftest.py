import os
import sys

# functions/ 配下のモジュール (jpx_loader, models など) を import できるようにする。
# main は auth.txt を読むため、テストからは import しない。
FUNCTIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'functions')
sys.path.insert(0, os.path.normpath(FUNCTIONS_DIR))
