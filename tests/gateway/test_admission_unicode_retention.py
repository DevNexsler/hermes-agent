"""Documented Unicode identity policy; native macOS storage proof stays host-specific."""
import unicodedata

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_workspace as kbw
from hermes_cli.kanban_db_spawn import _begin_launch
from tests.gateway import test_admission_retention_siblings as retention

home = retention.home


@pytest.mark.parametrize('operation', ['workspace', 'attachment', 'unlink', 'unrelated_control'])
def test_unicode_comparison_policy_retains_equivalent_native_input_keys(home, operation):
    """On Linux the spellings may name distinct files; this proves conservative key policy only."""
    kb.create_board('owned')
    first_path = home / 'Persönlich'
    alias_path = home / unicodedata.normalize('NFD', first_path.name)
    first_path.mkdir()
    alias_path.mkdir(exist_ok=True)
    blob = first_path / 'exact-input.txt'
    alias = alias_path / blob.name
    blob.write_bytes(b'EXACT original physical input')
    alias.write_bytes(b'EXACT original physical input')
    with kbc.connect_closing(board='owned') as conn:
        owner = kb.create_task(conn, title='EXACT equivalent key owner', assignee='owner',
                                workspace_kind='dir', workspace_path=str(alias_path))
        claimed = kb.claim_task(conn, owner)
        _begin_launch(conn, claimed, str(alias_path), 'owned')
        if operation == 'workspace':
            assert kbw._has_retained_workspace(conn, first_path)
        elif operation == 'attachment':
            kb.add_attachment(conn, owner, filename=alias.name, stored_path=str(alias))
            assert kbw._has_attachment_files(conn, first_path)
        else:
            if operation == 'unrelated_control':
                blob = home / 'unrelated-unowned-input.txt'
                blob.write_bytes(b'EXACT unowned input')
            with kb.write_txn(conn):
                kbw._unlink_unreferenced_attachment_blob(conn, blob)
            if operation == 'unrelated_control':
                assert not blob.exists()
            else:
                assert blob.read_bytes() == b'EXACT original physical input'


@pytest.mark.macos_only
@pytest.mark.parametrize('operation', ['workspace', 'blob'])
def test_native_macos_normalization_alias_keeps_retained_worker_input(home, operation):
    kb.create_board('owned')
    workspace = kb.workspaces_root(board='owned') / 'Persönlich'
    workspace.mkdir(parents=True)
    alias = workspace.with_name(unicodedata.normalize('NFD', workspace.name))
    if not alias.exists():
        pytest.skip('Temporary filesystem does not provide normalization-insensitive path aliases')
    assert alias.samefile(workspace)
    blob = workspace / 'exact-original-input.txt'
    blob.write_bytes(b'EXACT retained native filesystem input')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='settled scratch owner', workspace_kind='scratch',
                                workspace_path=str(workspace))
        second = kb.create_task(conn, title='EXACT shared native alias', assignee='owner',
                                 workspace_kind='dir', workspace_path=str(alias))
        claimed = kb.claim_task(conn, second)
        _begin_launch(conn, claimed, str(alias), 'owned')
        if operation == 'workspace':
            assert kb.complete_task(conn, first, summary='EXACT published result')
        else:
            aid = kb.add_attachment(conn, first, filename=blob.name, stored_path=str(blob))
            assert kb.delete_attachment(conn, aid) is not None
        assert (alias / blob.name).read_bytes() == b'EXACT retained native filesystem input'
