import os


def find(directory):
    # traverse root directory, and list directories as dirs and files as files
    result = []
    for root, _, files in os.walk(directory):
        for file in files:
            result.append(os.path.join(root, file))
    return result