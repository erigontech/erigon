#!/usr/bin/env python3
"""Convert meta.json files to _category_.json and add sidebar_position to frontmatter."""

import json
import os
import re
import sys
from pathlib import Path


DOCS_DIR = Path("/workspace/group/docusaurus-erigon/docs")


def add_sidebar_position(filepath: Path, position: int):
    """Add or update sidebar_position in frontmatter."""
    try:
        content = filepath.read_text(encoding='utf-8')
    except FileNotFoundError:
        return

    # Check if frontmatter exists
    if content.startswith('---'):
        end = content.find('---', 3)
        if end != -1:
            frontmatter = content[3:end]
            # Check if sidebar_position already exists
            if 'sidebar_position' in frontmatter:
                # Update existing
                frontmatter = re.sub(
                    r'sidebar_position:\s*\d+',
                    f'sidebar_position: {position}',
                    frontmatter
                )
            else:
                # Add at end of frontmatter
                frontmatter = frontmatter.rstrip('\n') + f'\nsidebar_position: {position}\n'
            new_content = '---' + frontmatter + '---' + content[end+3:]
            filepath.write_text(new_content, encoding='utf-8')
    else:
        # No frontmatter - add one
        new_content = f'---\nsidebar_position: {position}\n---\n\n' + content
        filepath.write_text(new_content, encoding='utf-8')


def process_meta_json(meta_path: Path, dir_position: int = None):
    """Process a single meta.json: create _category_.json and add sidebar positions to pages."""
    try:
        meta = json.loads(meta_path.read_text(encoding='utf-8'))
    except (json.JSONDecodeError, IOError) as e:
        print(f"Error reading {meta_path}: {e}")
        return

    parent_dir = meta_path.parent
    title = meta.get('title', parent_dir.name)
    pages = meta.get('pages', [])

    # Create _category_.json in this directory (not for the root docs dir)
    if parent_dir != DOCS_DIR:
        category = {"label": title}
        if dir_position is not None:
            category["position"] = dir_position
        category_path = parent_dir / '_category_.json'
        category_path.write_text(json.dumps(category, indent=2) + '\n', encoding='utf-8')
        print(f"Created: {category_path.relative_to(DOCS_DIR.parent)}")

    # Add sidebar_position to each page file
    for idx, page_name in enumerate(pages, start=1):
        # Try to find the file (could be .md, .mdx, or a directory with index)
        # First check if it's a subdir
        subdir = parent_dir / page_name
        if subdir.is_dir():
            # Find index file in that dir
            for ext in ['.md', '.mdx']:
                index_file = subdir / f'index{ext}'
                if index_file.exists():
                    add_sidebar_position(index_file, idx)
                    break
            # Also update the _category_.json position for that subdir's meta.json
            sub_meta = subdir / 'meta.json'
            if sub_meta.exists():
                process_meta_json(sub_meta, dir_position=idx)
        else:
            # Try as a file
            for ext in ['.md', '.mdx']:
                file_path = parent_dir / f'{page_name}{ext}'
                if file_path.exists():
                    add_sidebar_position(file_path, idx)
                    break

    # Delete meta.json
    meta_path.unlink()
    print(f"Deleted: {meta_path.relative_to(DOCS_DIR.parent)}")


def process_all():
    """Process all meta.json files, starting with root."""
    root_meta = DOCS_DIR / 'meta.json'
    if root_meta.exists():
        process_meta_json(root_meta, dir_position=None)
    else:
        # Process remaining meta.json files
        for meta_path in DOCS_DIR.rglob('meta.json'):
            process_meta_json(meta_path)


if __name__ == '__main__':
    process_all()
    print("Done!")
