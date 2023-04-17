import json
import sys


def strip_hex_prefix(hex_string):
    result = hex_string
    if hex_string.startswith('0x'):
        result = hex_string[2:]
    if len(result) < 16:
        result = '0' * (16 - len(result)) + result
    return result


def read_and_transform2():
    data = json.load(sys.stdin)
    return data

def read_and_transform():
    data = json.load(sys.stdin)

    transformed_data = {}

    for key, values in data.items():
        if len(values) == 12:
                capacity = ''.join([str(int(val, 16)) for val in values[-4:]])
                left = ''.join(map(strip_hex_prefix, values[3::-1]))
                right = ''.join(map(strip_hex_prefix, values[7:3:-1]))
                
                transformed_data[strip_hex_prefix(key)] = {'capacity': capacity, 'left': left, 'right': right}
        else:
            transformed_data[strip_hex_prefix(key)] = {'values': values}


    return transformed_data

def ensure_values(values):
    result = []

    for val in values[:8]:
        if val.startswith('0x'):
            r = int(val[2:], 16)
            result.append(hex(r))
        else:
            result.append(hex(int(val, 16)))

    return result

def strip_zeroes(s):
    if s.startswith('0x'):
        s = s[2:]
    result = s
    while result.startswith('0'):
        result = result[1:]
    return '0x' + result

def do_me_a_tree2(transformed_data, root, force_value_node = False):
    if not root.startswith('0x'):
        root = '0x' + root
    root_node_desc = transformed_data.get(root, None)
    if root_node_desc is None:
        root_node_desc = transformed_data.get(strip_zeroes(root), None)
        if root_node_desc is None:
            return {'type': "TerminalNode", 'hash': root }

    if len(root_node_desc) < 12 or force_value_node:
        return {'type': 'ValueNode', 'hash': root, 'values': ensure_values(root_node_desc)}

    else:
        values = root_node_desc
        capacity = ''.join([str(int(val, 16)) for val in values[-4:]])
        left = ''.join(map(strip_hex_prefix, values[3::-1]))
        right = ''.join(map(strip_hex_prefix, values[7:3:-1]))

    root_node = {
            'hash': root,
            'type': 'InnerNode',
            'left': do_me_a_tree2(transformed_data, '0x' + left, force_value_node = capacity != '0000'),
            'right': do_me_a_tree2(transformed_data, '0x' + right, force_value_node = capacity != '0000')
    }

    return root_node




def do_me_a_tree(transformed_data, root):
    root_node_desc = transformed_data.get(root, None)
    if root_node_desc is None:
        return "TERMINAL: " + root

    if root_node_desc.get('values', None) is not None:
        return {'hash': root, 'values': root_node_desc['values']}

    root_node = {
            'hash': root,
            'left': do_me_a_tree(transformed_data, root_node_desc['left']),
            'right': do_me_a_tree(transformed_data, root_node_desc['right'])
    }

    return root_node


def print_transformed_data(transformed_data):
    print(json.dumps(transformed_data, indent=4))


def main():
    transformed_data = read_and_transform2()
    print_transformed_data(do_me_a_tree2(transformed_data, sys.argv[1]))


if __name__ == "__main__":
    main()
