import json
import yaml
import sys


if __name__ == '__main__':
    doc = yaml.load(sys.stdin, Loader=yaml.Loader)
    print(json.dumps(doc, indent=2))
