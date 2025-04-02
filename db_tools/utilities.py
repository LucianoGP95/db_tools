import os, json
from datetime import datetime

def get_last_modified(path):
    """ Returns the last modified date of a file """
    try:
        stats = os.stat(path)
        return datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S")  # Last modified date
    except Exception as e:
        return {"error": str(e)}  # Handle any errors (e.g., permission errors)

def build_tree_json(directory, depth=0, max_depth=2, ignore_char=None):
    if depth >= max_depth:
        return None  # Stop recursion if depth limit is reached

    tree = {}
    try:
        for item in os.listdir(directory):
            if ignore_char and item.startswith(ignore_char):  
                continue  # Skip ignored files and folders

            path = os.path.join(directory, item)
            if os.path.isdir(path):
                tree[item] = build_tree_json(path, depth + 1, max_depth, ignore_char) or {}
            else:
                # Only return the last modified date
                tree[item] = get_last_modified(path)
    except PermissionError:
        tree["[ACCESS DENIED]"] = None  # Handle permission errors

    return tree

def draw_f_structure_json(root_path, max_depth=2, ignore_char=None):
    tree_structure = {root_path: build_tree_json(root_path, 0, max_depth, ignore_char)}
    return json.dumps(tree_structure, indent=4)

if __name__ == "__main__":
    directory = r"C:\Codebase\db_tools\project_example"  # Use raw string to avoid escape sequence issues
    json_output = draw_f_structure_json(directory, max_depth=2, ignore_char=".")
    print(json_output)

# Using treelib
""" from treelib import Tree

def build_tree_plain(directory, tree, parent=None, depth=0, max_depth=2, ignore_char=None):
    if depth >= max_depth:
        return  # Stop recursion if depth limit is reached

    for item in os.listdir(directory):
        if ignore_char and item.startswith(ignore_char):  
            continue  # Skip hidden files and folders

        path = os.path.join(directory, item)
        node_id = path
        tree.create_node(item, node_id, parent=parent)
        
        if os.path.isdir(path):
            build_tree_plain(path, tree, node_id, depth + 1, max_depth, ignore_char)

def draw_f_structure_plain(root_path, max_depth=2, ignore_char=None):
    tree = Tree()
    tree.create_node(root_path, root_path)  # Root node
    build_tree_plain(root_path, tree, root_path, 0, max_depth, ignore_char)
    tree.show()

if __name__ == "__main__":
    draw_f_structure_plain(directory, max_depth=2, ignore_char=".") """
