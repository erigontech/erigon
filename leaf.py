import json
import sys

def read_and_transform2():
    data = json.load(sys.stdin)
    return data

def print_value_nodes(data):
    root = data
    if root['type'] == 'ValueNode':
        print(json.dumps(root))
        return
    if root['type'] == 'TerminalNode':
        return
    print_value_nodes(root['left'])
    print_value_nodes(root['right'])


def main():
    data = read_and_transform2()
    print_value_nodes(data)


if __name__ == "__main__":
    main()
