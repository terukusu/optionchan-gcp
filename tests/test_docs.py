"""文書（README.md・docs/**/*.md）の静的検査（#140）。標準ライブラリだけで書く。

1. 相対リンクの先のファイルがあり、`#アンカー` が先の文書の見出しの ID（GitHub の規則）と一致する
2. README から 9 つの文書（setup・data・viewer・development・glossary・runbook・data-gaps・architecture の 2 つ）へのリンクがある
3. docs/runbook.md の先頭（最初の `##` 節より前）に、`##` 節すべてへのリンク（目次）がある
4. README と docs/ の文書（runbook 自身は除く）の「runbook の「節」の「小節」」の引用が runbook の見出しと一字一句一致する
   （「の」でつないだ 2 つ目以降は直前の見出しの節の中。「の」なしで並べたものは直前と同じ範囲で探す）
5. docs/data.md に infra/prod の optionchan_tokyo の表・ビュー・テーブル関数の名前と、主な表の schema の列名がすべて出る
6. README と docs/ の新しい文書のバッククォートで囲んだリポジトリのパスが実在する
7. README と docs/ の文書に `/home/` と、サービスアカウント（iam.gserviceaccount.com）以外のメールアドレスが無い
見出し・リンクはコードブロック（``` / ~~~ の中）を除いて数える。アンカーの規則は github_slug。
"""
import glob
import json
import os
import re
import unicodedata
from urllib.parse import unquote

import pytest

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
README = os.path.join(ROOT, 'README.md')
DOCS = os.path.join(ROOT, 'docs')
RUNBOOK = os.path.join(DOCS, 'runbook.md')
DATA = os.path.join(DOCS, 'data.md')
# README から辿れること（読み手ごとの入口。#140）
README_TARGETS = (
    'docs/setup.md', 'docs/data.md', 'docs/viewer.md', 'docs/development.md', 'docs/glossary.md',
    'docs/runbook.md', 'docs/data-gaps.md', 'docs/architecture/current.md', 'docs/architecture/target.md',
)
# #140 で書いた文書（パスの実在を検査する）
NEW_DOCS = ('README.md', 'docs/setup.md', 'docs/data.md', 'docs/viewer.md', 'docs/development.md', 'docs/glossary.md')
PATH_PREFIXES = ('apps/', 'sql/', 'infra/', 'schema/', 'monitoring/', 'docs/', 'tests/', '.github/', '.claude/')
# data.md の表の拾い方が壊れて 0 件で通らないように、拾った名前に含まれるべきもの
KNOWN_TABLES = {'option_price', 'atm_cm_iv', 'latest_runs', 'trading_calendar_latest', 'atm_mid_iv'}
SCHEMAS = (
    'schema/tokyo/option_price.json', 'schema/tokyo/future_price.json', 'schema/tokyo/spot_price.json',
    'schema/tokyo/ingest_runs.json', 'schema/tokyo/trading_calendar.json', 'schema/ohlc/atm_ohlc.json',
    'schema/cm/atm_cm_iv.json',
)
RUNBOOK_MIN_SECTIONS = 18
EMAIL_RE = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
# 「runbook の「X」」「docs/runbook.md の「X」の「Y」」「`docs/runbook.md` の「X」「Z」」
CITE_RE = re.compile(r'(?:docs/)?runbook(?:\.md)?`?\s*の((?:「[^」]+」の?)+)')
CITE_PART_RE = re.compile(r'「([^」]+)」(の?)')


def read(path):
    with open(path, encoding='utf-8') as f:
        return f.read()


def rel(path):
    return os.path.relpath(path, ROOT).replace(os.sep, '/')


def all_docs():
    """README.md と docs/**/*.md（パスの昇順）"""
    return [README] + sorted(glob.glob(os.path.join(DOCS, '**', '*.md'), recursive=True))


# ---- Markdown の読み取り ----

_FENCE_RE = re.compile(r'^\s{0,3}(`{3,}|~{3,})')


def strip_fences(text):
    """コードブロック（``` / ~~~ で囲んだ所）の行を空行にする（行番号は保つ）"""
    out = []
    fence = None
    for line in text.split('\n'):
        m = _FENCE_RE.match(line)
        if fence is None:
            if m:
                fence = m.group(1)[0] * len(m.group(1))
                out.append('')
                continue
            out.append(line)
        else:
            if m and m.group(1)[0] == fence[0] and len(m.group(1)) >= len(fence) and not line.strip()[len(m.group(1)):]:
                fence = None
            out.append('')
    return '\n'.join(out)


_CODE_SPAN_RE = re.compile(r'(`+)(.+?)\1')


def strip_code_spans(line):
    return _CODE_SPAN_RE.sub('', line)


_HEADING_RE = re.compile(r'^(#{1,6})\s+(.*?)\s*#*\s*$')


def headings(text):
    """(階層, 見出しの文字列) のリスト（コードブロックの中を除く。文書の順）"""
    out = []
    for line in strip_fences(text).split('\n'):
        m = _HEADING_RE.match(line)
        if m:
            out.append((len(m.group(1)), m.group(2)))
    return out


def heading_plain(text):
    """見出しの表示の文字列: リンクは文字だけ、インラインコードは中身だけ"""
    text = re.sub(r'!?\[([^\]]*)\]\([^)]*\)', r'\1', text)
    return _CODE_SPAN_RE.sub(r'\2', text)


def github_slug(text):
    """GitHub の見出しの ID（重複の番号なし）: 小文字化 → 文字・結合文字・数字・連結符・空白・- 以外を除く → 空白を -"""
    text = heading_plain(text).strip().lower()
    kept = ''.join(c for c in text
                   if c in ' -' or unicodedata.category(c)[0] in 'LMN' or unicodedata.category(c) == 'Pc')
    return kept.replace(' ', '-')


def anchors_of(heading_texts):
    """見出しの文字列の並び → ID の並び（重複は -1・-2… を付ける。GitHub と同じく文書の全階層で数える）"""
    seen = {}
    out = []
    for h in heading_texts:
        base = github_slug(h)
        slug = base
        if base in seen:
            seen[base] += 1
            slug = f'{base}-{seen[base]}'
        else:
            seen[base] = 0
        out.append(slug)
    return out


def doc_anchors(path):
    return set(anchors_of([h for _, h in headings(read(path))]))


_LINK_RE = re.compile(r'!?\[[^\]]*\]\(\s*<?([^)\s>]*)>?(?:\s+"[^"]*")?\s*\)')


def links(text):
    """(行番号, リンク先) のリスト。コードブロック・インラインコードの中を除く"""
    out = []
    for i, line in enumerate(strip_fences(text).split('\n'), 1):
        for m in _LINK_RE.finditer(strip_code_spans(line)):
            out.append((i, m.group(1)))
    return out


def is_external(target):
    return re.match(r'^[a-z][a-z0-9+.-]*:', target, re.I) is not None


def resolve(doc, target):
    """リンク先 → (リポジトリのパス（同じ文書なら doc 自身）, アンカー or None)"""
    path, _, anchor = target.partition('#')
    path = unquote(path)
    full = os.path.normpath(os.path.join(os.path.dirname(doc), path)) if path else doc
    return full, (unquote(anchor) if anchor else None)


# ---- アンカーの規則 ----

@pytest.mark.parametrize('heading, slug', [
    ('30 日固定の ATM IV（#100）', '30-日固定の-atm-iv100'),
    ('GitHub の設定（bootstrap の後、PR を出す前）', 'github-の設定bootstrap-の後pr-を出す前'),
    ('ATM IV（/atm_iv）', 'atm-ivatm_iv'),
    ('4. ビューアーの IAP', '4-ビューアーの-iap'),
    ('日次の品質チェックと新旧の突き合わせ（#40）', '日次の品質チェックと新旧の突き合わせ40'),
    ('`option_price`', 'option_price'),
    ('まず読むもの（テーブル関数）', 'まず読むものテーブル関数'),
])
def test_github_slug(heading, slug):
    assert github_slug(heading) == slug


def test_anchor_duplicates():
    assert anchors_of(['変数', 'コスト', '変数', '変数']) == ['変数', 'コスト', '変数-1', '変数-2']


def test_headings_skip_code_blocks():
    text = '# 題\n\n```sh\n# コメント\n```\n\n## 節\n~~~\n## 例\n~~~\n'
    assert headings(text) == [(1, '題'), (2, '節')]


def test_links_skip_code():
    text = '[a](x.md) `[b](y.md)`\n```\n[c](z.md)\n```\n![d](img.png "t")'
    assert [t for _, t in links(text)] == ['x.md', 'img.png']


# ---- 1. リンク ----

def test_links_resolve():
    errors = []
    anchor_cache = {}
    for doc in all_docs():
        for lineno, target in links(read(doc)):
            if not target or is_external(target):
                continue
            full, anchor = resolve(doc, target)
            where = f'{rel(doc)}:{lineno} {target}'
            if not os.path.exists(full):
                errors.append(f'{where}: 先が無い')
                continue
            if anchor is None:
                continue
            if not full.endswith('.md'):
                errors.append(f'{where}: .md 以外へのアンカー')
                continue
            if full not in anchor_cache:
                anchor_cache[full] = doc_anchors(full)
            if anchor not in anchor_cache[full]:
                errors.append(f'{where}: アンカーが無い')
    assert not errors, '\n'.join(errors)


# ---- 2. README から各文書へ ----

def test_readme_links_docs():
    found = set()
    for _, target in links(read(README)):
        if target and not is_external(target):
            found.add(rel(resolve(README, target)[0]))
    missing = [t for t in README_TARGETS if t not in found]
    assert not missing, missing


# ---- 3. runbook の目次 ----

def test_runbook_toc():
    text = read(RUNBOOK)
    hs = headings(text)
    anchors = anchors_of([h for _, h in hs])
    sections = [(h, a) for (level, h), a in zip(hs, anchors) if level == 2]
    assert len(sections) >= RUNBOOK_MIN_SECTIONS, len(sections)
    # 最初の ## 行より前（コードブロックを除く）
    lines = strip_fences(text).split('\n')
    first = next(i for i, line in enumerate(lines) if line.startswith('## '))
    head = '\n'.join(lines[:first])
    toc = {unquote(t[1:]) for _, t in links(head) if t.startswith('#')}
    missing = [h for h, a in sections if a not in toc]
    assert not missing, missing


# ---- 4. runbook の引用 ----

def section_range(hs, index):
    """見出し hs[index] の節の範囲（次の同じか上の階層の見出しまで）の (start, end)"""
    level = hs[index][0]
    end = next((j for j in range(index + 1, len(hs)) if hs[j][0] <= level), len(hs))
    return index + 1, end


def check_citation(hs, names, joins):
    """names[i] を探す。joins[i] が True（直前と「の」でつながる）なら直前の見出しの節の中、False なら直前と同じ範囲。
    見つからなければその名前を返す"""
    scope = (0, len(hs))
    prev = None
    for name, nested in zip(names, joins):
        if nested and prev is not None:
            scope = section_range(hs, prev)
        idx = next((j for j in range(*scope) if 2 <= hs[j][0] <= 4 and hs[j][1] == name), None)
        if idx is None:
            return name
        prev = idx
    return None


def citations(text):
    """(行番号, 名前の並び, 「の」でつながるかの並び)。コードブロックの中を除く"""
    out = []
    for i, line in enumerate(strip_fences(text).split('\n'), 1):
        for m in CITE_RE.finditer(line):
            parts = CITE_PART_RE.findall(m.group(1))
            names = [p[0] for p in parts]
            joins = [False] + [bool(p[1]) for p in parts[:-1]]
            out.append((i, names, joins))
    return out


def test_citation_parser():
    hs = [(2, 'A'), (3, '変数'), (2, 'B'), (3, 'コスト'), (3, '変数')]
    assert citations('runbook の「A」の「変数」')[0][1:] == (['A', '変数'], [False, True])
    assert citations('`docs/runbook.md` の「A」「B」')[0][1:] == (['A', 'B'], [False, False])
    assert check_citation(hs, ['A', '変数'], [False, True]) is None
    assert check_citation(hs, ['A', 'コスト'], [False, True]) == 'コスト'
    assert check_citation(hs, ['A', 'B'], [False, False]) is None
    assert check_citation(hs, ['B', 'コスト', '変数'], [False, True, False]) is None
    assert check_citation(hs, ['回収と照合'], [False]) == '回収と照合'


def test_runbook_citations():
    hs = headings(read(RUNBOOK))
    errors = []
    count = 0
    for doc in all_docs():
        if doc == RUNBOOK:
            continue
        for lineno, names, joins in citations(read(doc)):
            count += 1
            bad = check_citation(hs, names, joins)
            if bad is not None:
                cited = ''.join(('の' if j else '') + f'「{n}」' for n, j in zip(names, joins))
                errors.append(f'{rel(doc)}:{lineno} {cited}: 「{bad}」が無い')
    # 拾い方が壊れて 0 件で通らないように（docs/data-gaps.md だけで 7 件ある）
    assert count >= 7, count
    assert not errors, '\n'.join(errors)


# ---- 5. data.md の網羅 ----

def hcl_blocks(text, kinds):
    """resource "<kind>" "<name>" { … } のブロックの本文のリスト（文字列・コメントの中の括弧は数えない）"""
    out = []
    for m in re.finditer(r'^resource\s+"(%s)"\s+"[^"]+"\s*\{' % '|'.join(kinds), text, re.M):
        depth, i, in_str = 1, m.end(), False
        while i < len(text) and depth:
            c = text[i]
            if in_str:
                if c == '\\':
                    i += 1
                elif c == '"':
                    in_str = False
            elif c == '"':
                in_str = True
            elif c == '#' or text.startswith('//', i):
                i = text.find('\n', i)
                if i < 0:
                    break
            elif c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
            i += 1
        out.append(text[m.end():i - 1])
    return out


def for_each_keys(block):
    """for_each の名前: toset([...]) の文字列、{ key = ... } の map のキー、local.bq.tables（tables.json の tables）"""
    m = re.search(r'^\s*for_each\s*=\s*(.*)$', block, re.M)
    if not m:
        return None
    rhs = m.group(1).strip()
    if rhs.startswith('toset('):
        return re.findall(r'"([^"]+)"', rhs)
    if rhs == 'local.bq.tables':
        return list(json.loads(read(os.path.join(ROOT, 'schema', 'tokyo', 'tables.json')))['tables'])
    if rhs.startswith('{'):
        start = block.index('{', m.start(1))
        depth, j = 0, start
        while True:
            if block[j] == '{':
                depth += 1
            elif block[j] == '}':
                depth -= 1
                if not depth:
                    break
            j += 1
        return re.findall(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=', block[start + 1:j], re.M)
    raise AssertionError(f'未対応の for_each: {rhs}')


def tokyo_objects():
    """infra/prod/*.tf の optionchan_tokyo の表・ビュー・テーブル関数の名前"""
    names = set()
    for tf in sorted(glob.glob(os.path.join(ROOT, 'infra', 'prod', '*.tf'))):
        for block in hcl_blocks(read(tf), ('google_bigquery_table', 'google_bigquery_routine')):
            if 'google_bigquery_dataset.tokyo' not in block and 'local.bq.dataset' not in block:
                continue
            m = re.search(r'^\s*(?:table_id|routine_id)\s*=\s*(\S+)', block, re.M)
            assert m, (tf, block[:80])
            value = m.group(1)
            if value.startswith('"'):
                names.add(value.strip('"'))
            elif value == 'each.key':
                keys = for_each_keys(block)
                assert keys, (tf, block[:80])
                names.update(keys)
            else:
                raise AssertionError(f'{rel(tf)}: 未対応の名前 {value}')
    return names


def schema_columns(path):
    def walk(fields):
        for f in fields:
            yield f['name']
            yield from walk(f.get('fields', []))

    return list(walk(json.loads(read(os.path.join(ROOT, path)))))


def mentions(text, word):
    return re.search(r'(?<![A-Za-z0-9_])%s(?![A-Za-z0-9_])' % re.escape(word), text) is not None


def test_tokyo_objects_parser():
    names = tokyo_objects()
    assert KNOWN_TABLES <= names, KNOWN_TABLES - names


def test_data_md_tables():
    names = tokyo_objects()
    text = read(DATA)
    missing = sorted(n for n in names if not mentions(text, n))
    assert not missing, missing


@pytest.mark.parametrize('schema', SCHEMAS)
def test_data_md_columns(schema):
    columns = schema_columns(schema)
    assert columns, schema
    text = read(DATA)
    missing = [c for c in columns if not mentions(text, c)]
    assert not missing, (schema, missing)


# ---- 6. 新しい文書のパスの実在 ----

def expand_braces(path):
    m = re.search(r'\{([^{}]*)\}', path)
    if not m:
        return [path]
    out = []
    for alt in m.group(1).split(','):
        out.extend(expand_braces(path[:m.start()] + alt + path[m.end():]))
    return out


def repo_paths(text):
    """バッククォートで囲んだリポジトリのパス（(行番号, パス)）。空白（コマンド）・<…>（プレースホルダ）を含むものは除く。
    末尾の :行番号・#アンカー・?クエリは落とす"""
    out = []
    for i, line in enumerate(strip_fences(text).split('\n'), 1):
        for m in _CODE_SPAN_RE.finditer(line):
            s = m.group(2).strip()
            if not s.startswith(PATH_PREFIXES) or re.search(r'\s|<|>', s):
                continue
            s = re.sub(r'[#?].*$', '', s)
            s = re.sub(r':\d+(?:[-,]\d+)*$', '', s)
            out.append((i, s))
    return out


def path_exists(path):
    for p in expand_braces(path):
        full = os.path.join(ROOT, p)
        if not (glob.glob(full) if any(c in p for c in '*?[') else os.path.exists(full)):
            return False
    return True


def test_repo_paths_parser():
    text = '`apps/viewer` `sql/latest/*.sql` `tests/test_x.py:71` `apps/<app>/src` `apps/viewer で uv run` `x/y`'
    assert [p for _, p in repo_paths(text)] == ['apps/viewer', 'sql/latest/*.sql', 'tests/test_x.py']
    assert expand_braces('infra/prod/{sq,gaps}.tf') == ['infra/prod/sq.tf', 'infra/prod/gaps.tf']


@pytest.mark.parametrize('doc', NEW_DOCS)
def test_new_doc_paths_exist(doc):
    path = os.path.join(ROOT, doc)
    assert os.path.exists(path), f'{doc} が無い'
    missing = [f'{doc}:{i} {p}' for i, p in repo_paths(read(path)) if not path_exists(p)]
    assert not missing, '\n'.join(missing)


# ---- 7. 個人の値の形 ----

def test_no_personal_values():
    errors = []
    for doc in all_docs():
        for i, line in enumerate(read(doc).split('\n'), 1):
            if '/home/' in line:
                errors.append(f'{rel(doc)}:{i}: /home/')
            for m in EMAIL_RE.finditer(line):
                if not m.group(0).endswith('iam.gserviceaccount.com'):
                    errors.append(f'{rel(doc)}:{i}: メールアドレス')
    # 値そのものは出さない（行番号だけ）
    assert not errors, '\n'.join(errors)
