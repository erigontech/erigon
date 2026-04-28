#!/usr/bin/env python3
"""Convert Fumadocs MDX files to Docusaurus-compatible format."""

import os
import re
import sys
from pathlib import Path


DOCS_DIR = Path("/workspace/group/docusaurus-erigon/docs")

# Callout type mapping
CALLOUT_TYPE_MAP = {
    "warn": "warning",
    "warning": "warning",
    "info": "info",
    "tip": "tip",
    "error": "danger",
    "success": "tip",
}


def convert_callouts(content: str) -> str:
    """Convert <Callout type="...">text</Callout> to Docusaurus admonitions."""

    def replace_callout(m):
        type_attr = m.group(1)
        if type_attr is None:
            admonition = "info"
        else:
            admonition = CALLOUT_TYPE_MAP.get(type_attr.lower(), "info")
        inner = m.group(2).strip()
        return f":::{admonition}\n{inner}\n:::"

    # With type attribute
    content = re.sub(
        r'<Callout\s+type=["\'](\w+)["\']>\s*(.*?)\s*</Callout>',
        replace_callout,
        content,
        flags=re.DOTALL,
    )
    # Without type attribute
    content = re.sub(
        r'<Callout>\s*(.*?)\s*</Callout>',
        lambda m: f":::info\n{m.group(1).strip()}\n:::",
        content,
        flags=re.DOTALL,
    )
    return content


def convert_tabs(content: str) -> str:
    """Convert Fumadocs Tabs/Tab to Docusaurus Tabs/TabItem."""
    # Convert <Tabs items={[...]}>  →  <Tabs>
    content = re.sub(r'<Tabs\s+items=\{[^}]*\}>', '<Tabs>', content)

    # Convert <Tab value="Linux"> → <TabItem value="linux" label="Linux">
    def replace_tab_open(m):
        label = m.group(1)
        value = label.lower().replace(" ", "-").replace("/", "-")
        return f'<TabItem value="{value}" label="{label}">'

    content = re.sub(r'<Tab\s+value=["\']([^"\']+)["\']>', replace_tab_open, content)

    # Convert </Tab> → </TabItem>
    content = content.replace('</Tab>', '</TabItem>')

    return content


def convert_cards(content: str) -> str:
    """Convert Fumadocs Card/Cards to markdown links."""
    # Remove <Cards> and </Cards>
    content = re.sub(r'<Cards>\s*', '', content)
    content = re.sub(r'\s*</Cards>', '', content)

    # <Card href="..." title="..." /> → - [title](href)
    content = re.sub(
        r'<Card\s+href=["\']([^"\']+)["\'](\s+title=["\']([^"\']+)["\'])?\s*/>',
        lambda m: f'- [{m.group(3) or m.group(1)}]({m.group(1)})',
        content,
    )

    # <Card href="...">text</Card> → - [text](href)
    content = re.sub(
        r'<Card\s+href=["\']([^"\']+)["\']>\s*(.*?)\s*</Card>',
        lambda m: f'- [{m.group(2).strip()}]({m.group(1)})',
        content,
        flags=re.DOTALL,
    )

    # Also handle title before href
    content = re.sub(
        r'<Card\s+title=["\']([^"\']+)["\']\s+href=["\']([^"\']+)["\']\s*/>',
        lambda m: f'- [{m.group(1)}]({m.group(2)})',
        content,
    )

    return content


def remove_fumadocs_imports(content: str) -> str:
    """Remove fumadocs import lines."""
    lines = content.split('\n')
    filtered = []
    for line in lines:
        if re.match(r"^import\s+.*from\s+['\"]fumadocs-(?:ui|core|mdx)/", line):
            continue
        filtered.append(line)
    return '\n'.join(filtered)


def add_docusaurus_tab_imports(content: str) -> str:
    """Add Docusaurus tab imports if the file uses tabs."""
    if '<Tabs>' not in content and '<TabItem' not in content:
        return content

    # Check if imports already present
    if "from '@theme/Tabs'" in content or 'from "@theme/Tabs"' in content:
        return content

    tab_imports = "import Tabs from '@theme/Tabs';\nimport TabItem from '@theme/TabItem';"

    # Add after frontmatter if present
    if content.startswith('---'):
        # Find end of frontmatter
        end = content.find('---', 3)
        if end != -1:
            end_of_frontmatter = end + 3
            before = content[:end_of_frontmatter]
            after = content[end_of_frontmatter:]
            # Strip leading newlines from after
            after = after.lstrip('\n')
            return before + '\n\n' + tab_imports + '\n\n' + after
    return tab_imports + '\n\n' + content


def has_jsx(content: str) -> bool:
    """Check if content has JSX (import statements or uppercase component tags)."""
    # Check for import statements
    if re.search(r'^import\s+', content, re.MULTILINE):
        return True
    # Check for uppercase starting tags like <ComponentName
    if re.search(r'<[A-Z][a-zA-Z]+[\s/>]', content):
        return True
    return False


def convert_file(filepath: Path) -> str:
    """Convert a single MDX/MD file."""
    content = filepath.read_text(encoding='utf-8')

    original = content

    # Step 1: Remove fumadocs imports
    content = remove_fumadocs_imports(content)

    # Step 2: Convert Callouts
    content = convert_callouts(content)

    # Step 3: Convert Tabs
    content = convert_tabs(content)

    # Step 4: Convert Cards
    content = convert_cards(content)

    # Step 5: Add Docusaurus tab imports if needed
    content = add_docusaurus_tab_imports(content)

    # Write back
    if content != original:
        filepath.write_text(content, encoding='utf-8')

    return content


def maybe_rename_to_md(filepath: Path, content: str):
    """Rename .mdx → .md if no JSX remains."""
    if filepath.suffix != '.mdx':
        return filepath
    if not has_jsx(content):
        new_path = filepath.with_suffix('.md')
        filepath.rename(new_path)
        return new_path
    return filepath


def process_all():
    """Process all MDX/MD files under docs/."""
    mdx_files = list(DOCS_DIR.rglob("*.mdx")) + list(DOCS_DIR.rglob("*.md"))
    print(f"Found {len(mdx_files)} files to process")

    converted = 0
    renamed = 0

    for filepath in mdx_files:
        content = convert_file(filepath)
        new_path = maybe_rename_to_md(filepath, content)
        converted += 1
        if new_path != filepath:
            renamed += 1

    print(f"Converted: {converted} files")
    print(f"Renamed .mdx → .md: {renamed} files")


if __name__ == '__main__':
    process_all()
