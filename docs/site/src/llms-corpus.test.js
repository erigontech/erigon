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

function pages(text) {
  // Split on the URL line. A column-zero "# comment" inside a fence is not a
  // heading, and splitting on "# " would truncate the page being measured.
  const parts = text.split(/^URL: (\S+)$/m)
  const out = []
  for (let i = 1; i < parts.length; i += 2) out.push([parts[i], parts[i + 1]])
  return out
}

const parse = md =>
  fromMarkdown(md, {extensions: [gfm()], mdastExtensions: [gfmFromMarkdown()]})

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

  const captured = nodesOfType(tree, 'code')
    .filter((c) => /^URL: https:\/\//m.test(c.value))
  assert.deepEqual(captured.map((c) => c.value.match(/^URL: \S+/m)[0]), [],
    'a fence absorbed a page sentinel')

  // GFM autolinks the bare URL, so the paragraph is text("URL: ") + link(...)
  // rather than one text node.
  const flat = (n) =>
    (n.children ?? []).map((c) => c.value ?? c.url ?? flat(c)).join('')
  const urlParagraphs = tree.children
    .filter((n) => n.type === 'paragraph')
    .map(flat)
    .filter((v) => v.startsWith('URL: https://'))
  const h1s = tree.children.filter((n) => n.type === 'heading' && n.depth === 1)
  assert.equal(urlParagraphs.length, h1s.length,
    'a page lost its title or its URL line')
  assert.ok(urlParagraphs.length > 0, 'no page sentinels found at all')

  // Every page the index lists must still be a page here. llms.txt also links
  // out to GitHub and Docker Hub, which are not pages, so only the doc URLs
  // are compared.
  const index = fs.readFileSync(
    path.join(HERE, '..', 'static', 'llms.txt'), 'utf8')
  const listed = [...index.matchAll(/^- \[[^\]]*\]\((https:\/\/docs\.erigon\.tech[^)]*)\)/gm)]
    .map((m) => m[1])
  const present = new Set(urlParagraphs.map((v) => v.slice('URL: '.length)))
  assert.deepEqual(listed.filter((u) => !present.has(u)), [],
    'a page listed in llms.txt is missing from llms-full.txt')
})

test('every fenced block in the corpus closes', () => {
  const text = fs.readFileSync(CORPUS, 'utf8')
  const unclosed = []
  for (const [url, body] of pages(text)) {
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

test('an indented fence produces code that is inside a list item', () => {
  // The weaker form of this check compared the opening and closing columns,
  // which a uniformly under-indented fence satisfies while the parser still
  // puts its code outside the item. Ask the parser instead: a fence the
  // generator indented is only doing its job if the code node it produces is
  // inside a listItem. A column-1 fence is top-level on purpose.
  const text = fs.readFileSync(CORPUS, 'utf8')
  const escaped = []
  for (const [url, body] of pages(text)) {
    const tree = parse(body)
    const inItem = new Set()
    for (const item of nodesOfType(tree, 'listItem')) {
      for (const code of nodesOfType(item, 'code')) inItem.add(code)
    }
    for (const code of nodesOfType(tree, 'code')) {
      const col = code.position?.start?.column ?? 1
      if (col > 1 && !inItem.has(code)) {
        escaped.push(`${url}: fence at column ${col} is not inside its item`)
      }
    }
  }
  assert.deepEqual(escaped, [], 'an indented fence left its list item')
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
