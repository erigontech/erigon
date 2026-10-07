// Parses the committed corpus with the same Markdown engine the docs use, and
// asserts the structure a text-level check cannot see.
//
// Word, line and blank-line counts cannot see a fence escaping its list item:
// the text is identical either way, while the closing fence opens a block of its
// own and the rest of the page becomes code. Structure has to be asserted
// against a parse, not against text metrics.
import {test} from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import path from 'node:path'
// import.meta.dirname would need Node 20.11; package.json declares >=20.0.
import {fileURLToPath} from 'node:url'
import {fromMarkdown} from 'mdast-util-from-markdown'
import {gfm} from 'micromark-extension-gfm'
import {gfmFromMarkdown} from 'mdast-util-gfm'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const CORPUS = path.join(HERE, '..', 'static', 'llms-full.txt')

const parse = md =>
  fromMarkdown(md, {extensions: [gfm()], mdastExtensions: [gfmFromMarkdown()]})

function pages(text) {
  // Boundaries come from the parse, never from a regex over the text. Splitting
  // on "# " truncates a page at a column-zero comment inside a fence, and
  // splitting on "URL: " truncates it at a configuration example containing one
  // — either cut lands mid-fence and hands the tests two fragments that report a
  // closed fence as unclosed. A heading node is one the parser actually saw, so
  // a "# " or "URL: " inside a fence is part of that code node and is never a
  // boundary. These are also the positions the assembler starts each page at.
  const lines = text.split('\n')
  const starts = parse(text).children
    .filter((n) => n.type === 'heading' && n.depth === 1)
    .map((n) => n.position.start.line - 1)
  return starts.map((from, i) => {
    const to = i + 1 < starts.length ? starts[i + 1] : lines.length
    const body = lines.slice(from, to).join('\n')
    // Read the sentinel positionally — the first non-blank line after the
    // heading — rather than searching the body for the first "URL:" line. The
    // search would label the page with a configuration example when a page has
    // lost its own sentinel, which is exactly the case the label has to name
    // correctly. The heading stands in when there is nothing sentinel-shaped
    // there; the caller only uses this to label a failure.
    const first = lines.slice(from + 1, to).find((l) => l.trim() !== '') ?? ''
    const m = first.match(/^URL: (\S+)$/)
    return [m ? m[1] : lines[from].slice(0, 60), body]
  })
}

// Only a URL the index lists is a page sentinel. A documentation paragraph that
// happens to start "URL: https://" — a configuration example written as prose,
// outside any fence — is page content, and counting it as a boundary fails a
// corpus in which nothing was lost.
function topLevelSentinels(tree, sentinel) {
  // GFM autolinks the bare URL, so the paragraph is text("URL: ") + link(...)
  // rather than one text node.
  const flat = (n) =>
    (n.children ?? []).map((c) => c.value ?? c.url ?? flat(c)).join('')
  return tree.children
    .filter((n) => n.type === 'paragraph')
    .map((n) => flat(n).trim())
    .filter((v) => sentinel.has(v))
}

function nodesOfType(tree, type) {
  const found = []
  const walk = n => {
    if (n.type === type) found.push(n)
    ;(n.children || []).forEach(walk)
  }
  walk(tree)
  return found
}

test('every page sentinel survives as prose, none captured by a fence', () => {
  // Parse the whole file once and never split it. Splitting on the "URL:" line
  // consumes the delimiters this asserts on, which makes the assertion
  // unfalsifiable; on the intact file a swallowed page shows up directly, as a
  // sentinel that has become part of a code node.
  const text = fs.readFileSync(CORPUS, 'utf8')
  const tree = parse(text)

  // Only the URLs the index lists are page sentinels. A configuration example
  // containing a line like "URL: https://rpc.example" is documentation, not a
  // captured page, and must not fail the build.
  const index = fs.readFileSync(
    path.join(HERE, '..', 'static', 'llms.txt'), 'utf8')
  const listed = [...index.matchAll(/^- \[[^\]]*\]\((https:\/\/docs\.erigon\.tech[^)]*)\)/gm)]
    .map((m) => m[1])
  const sentinel = new Set(listed.map((u) => `URL: ${u}`))

  const captured = []
  for (const code of nodesOfType(tree, 'code')) {
    for (const line of code.value.split('\n')) {
      if (sentinel.has(line.trim())) captured.push(line.trim())
    }
  }
  assert.deepEqual(captured, [], 'a fence absorbed a page sentinel')

  const urlParagraphs = topLevelSentinels(tree, sentinel)
  const h1s = tree.children.filter((n) => n.type === 'heading' && n.depth === 1)
  assert.equal(urlParagraphs.length, h1s.length,
    'a page lost its title or its URL line')
  assert.ok(urlParagraphs.length > 0, 'no page sentinels found at all')

  const present = new Set(urlParagraphs.map((v) => v.slice('URL: '.length)))
  assert.deepEqual(listed.filter((u) => !present.has(u)), [],
    'a page listed in llms.txt is missing from llms-full.txt')
})

test('a documented URL line is not counted as a page sentinel', () => {
  const listed = ['https://docs.erigon.tech/getting-started']
  const sentinel = new Set(listed.map((u) => `URL: ${u}`))
  const text = ['# Getting started',
                '',
                'URL: https://docs.erigon.tech/getting-started',
                '',
                'Point a client at your own node:',
                '',
                'URL: https://rpc.example',
                ''].join('\n')
  assert.deepEqual(topLevelSentinels(parse(text), sentinel),
                   ['URL: https://docs.erigon.tech/getting-started'],
                   'a documentation URL was counted as a page boundary')
})

test('every fenced block in the corpus closes', () => {
  // Splitting mid-fence would report both halves as unclosed, so the boundaries
  // have to be the parse-derived ones pages() returns.
  const text = fs.readFileSync(CORPUS, 'utf8')
  const parsed = pages(text)
  assert.ok(parsed.length > 0, 'no page headings found')

  const unclosed = []
  for (const [url, body] of parsed) {
    let open = null
    for (const line of body.split('\n')) {
      const m = line.match(/^\s*(`{3,}|~{3,})/)
      if (!m) continue
      if (open === null) open = m[1]
      else if (m[1][0] === open[0] && m[1].length >= open.length) open = null
    }
    if (open !== null) unclosed.push(url)
  }
  assert.deepEqual(unclosed, [], 'page ends inside a fenced block')
})

test('an indented fence produces code inside the container it belongs to', () => {
  // Comparing the opening and closing columns alone passes a uniformly
  // under-indented fence whose code the parser still puts outside the item, so
  // ask the parser for the container instead. Indentation can come from a list
  // item or from a blockquote marker, and both are legitimate: what must not
  // happen is an indented fence landing at the top level.
  const text = fs.readFileSync(CORPUS, 'utf8')
  const escaped = []
  for (const [url, body] of pages(text)) {
    const contained = new Set()
    const walk = (node, depth) => {
      const inside = depth || node.type === 'listItem' || node.type === 'blockquote'
      if (inside && node.type === 'code') contained.add(node)
      ;(node.children ?? []).forEach((c) => walk(c, inside))
    }
    const tree = parse(body)
    walk(tree, false)
    for (const code of nodesOfType(tree, 'code')) {
      const col = code.position?.start?.column ?? 1
      if (col > 1 && !contained.has(code)) {
        escaped.push(`${url}: fence at column ${col} is in no container`)
      }
    }
  }
  assert.deepEqual(escaped, [], 'an indented fence left its container')
})

test('a fence closes at the column it opened at', () => {
  // Complementary to the check above, and it catches a different shape: a fence
  // whose opener is indented into its item while its closer sits at column 0.
  // The parser may still nest that code node, so containment alone passes it,
  // but the closer has ended the item and the fence runs on.
  const text = fs.readFileSync(CORPUS, 'utf8')
  const mismatched = []
  for (const [url, body] of pages(text)) {
    const lines = body.split('\n')
    for (let i = 0; i < lines.length; i++) {
      const open = lines[i].match(/^([ \t]*)(`{3,}|~{3,})\s*\S*\s*$/)
      if (!open) continue
      for (let j = i + 1; j < lines.length; j++) {
        const close = lines[j].match(/^([ \t]*)(`{3,}|~{3,})[ \t]*$/)
        if (!close) continue
        if (close[2][0] === open[2][0] && close[2].length >= open[2].length) {
          if (close[1].length !== open[1].length) {
            mismatched.push(
              `${url}: opens at ${open[1].length}, closes at ${close[1].length}`)
          }
          i = j
          break
        }
      }
    }
  }
  assert.deepEqual(mismatched, [], 'a fence closed at a different column')
})
