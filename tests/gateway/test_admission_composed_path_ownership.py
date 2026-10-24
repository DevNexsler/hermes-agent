"""Original filesystem paths stay distinct from Unicode keys and inode aliases."""
import errno
import os
from pathlib import Path
import unicodedata

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_workspace as kbw
from hermes_cli.kanban_db_spawn import _begin_launch
from tests.gateway import test_admission_retention_siblings as retention

home = retention.home


@pytest.mark.require_symlinks
@pytest.mark.parametrize('shape', ['directory', 'filename'])
@pytest.mark.parametrize('operation', ['delete', 'failed_cleanup', 'staged_cleanup'])
@pytest.mark.parametrize('state', ['pending', 'settled'])
def test_unicode_key_never_follows_different_sibling_symlink(home, shape, operation, state):
    kb.create_board('owned')
    unrelated = home / 'unrelated'
    unrelated.mkdir()
    witness = unrelated / 'input.txt'
    witness.write_bytes(b'EXACT unrelated bytes')
    nfd = unicodedata.normalize('NFD', 'Café')
    if shape == 'directory':
        directory = home / nfd
        directory.mkdir()
        blob = directory / 'input.txt'
        sibling = home / 'Café'
        target = unrelated
    else:
        directory = home / 'original-inputs'
        directory.mkdir()
        blob = directory / (nfd + '.txt')
        sibling = directory / 'Café.txt'
        target = witness
    blob.write_bytes(b'EXACT original retained input')
    if sibling.exists():
        pytest.skip('Composition requires distinct NFC/NFD names on this filesystem')
    sibling.symlink_to(target, target_is_directory=shape == 'directory')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='settled attachment owner')
        assert kb.complete_task(conn, first, summary='EXACT published result')
        second = kb.create_task(conn, title='EXACT original-path input owner', assignee='owner',
                                 workspace_kind='dir', workspace_path=str(directory))
        claimed = kb.claim_task(conn, second)
        if state == 'pending':
            _begin_launch(conn, claimed, str(directory), 'owned')
        else:
            assert kb.complete_task(conn, second, summary='settled consumer',
                                    expected_run_id=claimed.current_run_id)
        if operation == 'delete':
            aid = kb.add_attachment(conn, first, filename=blob.name, stored_path=str(blob))
            assert kb.delete_attachment(conn, aid) is not None
        elif operation == 'failed_cleanup':
            kbw.cleanup_failed_attachment_blob(conn, blob)
        else:
            kb._discard_staged_copies(conn, [blob], directory)
        if state == 'pending':
            assert blob.read_bytes() == b'EXACT original retained input'
        else:
            assert not blob.exists()
        assert sibling.is_symlink() and witness.read_bytes() == b'EXACT unrelated bytes'


@pytest.mark.parametrize('shared_inode', [True, False])
def test_attachment_alias_reference_uses_actual_file_identity(home, shared_inode):
    kb.create_board('owned')
    blob, alias = home / 'original.txt', home / 'other-reference.txt'
    blob.write_bytes(b'EXACT bytes')
    if shared_inode:
        try:
            os.link(blob, alias)
        except OSError as exc:
            if exc.errno not in (errno.EPERM, errno.EACCES, errno.EXDEV, errno.ENOSYS, errno.ENOTSUP):
                raise
            pytest.skip(f'Temporary filesystem cannot create hard links: {exc}')
        assert blob.samefile(alias)
    else:
        alias.write_bytes(b'EXACT bytes')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='settled first owner')
        second = kb.create_task(conn, title='settled second metadata owner')
        assert kb.complete_task(conn, first, summary='first result')
        assert kb.complete_task(conn, second, summary='second result')
        aid = kb.add_attachment(conn, first, filename=blob.name, stored_path=str(blob))
        kb.add_attachment(conn, second, filename=alias.name, stored_path=str(alias))
        assert kb.delete_attachment(conn, aid) is not None
        assert blob.exists() is shared_inode
        assert alias.read_bytes() == b'EXACT bytes'


def test_unreadable_reference_identity_preserves_blob_and_metadata(home, monkeypatch):
    kb.create_board('owned')
    blob, alias = home / 'original.txt', home / 'other-reference.txt'
    blob.write_bytes(b'EXACT original bytes')
    alias.write_bytes(b'EXACT unrelated bytes')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='settled first owner')
        second = kb.create_task(conn, title='settled reference owner')
        assert kb.complete_task(conn, first, summary='first result')
        assert kb.complete_task(conn, second, summary='second result')
        aid = kb.add_attachment(conn, first, filename=blob.name, stored_path=str(blob))
        ref = kb.add_attachment(conn, second, filename=alias.name, stored_path=str(alias))
        samefile = Path.samefile
        def unreadable(left, right):
            if left == alias and right == blob:
                raise PermissionError('inert unreadable reference identity')
            return samefile(left, right)
        monkeypatch.setattr(Path, 'samefile', unreadable)
        with pytest.raises(PermissionError, match='unreadable reference identity'):
            kb.delete_attachment(conn, aid)
        assert kb.get_attachment(conn, aid) is not None and kb.get_attachment(conn, ref) is not None
        assert blob.read_bytes() == b'EXACT original bytes'


@pytest.mark.macos_only
@pytest.mark.parametrize('operation', ['workspace', 'blob', 'attachment'])
def test_native_macos_case_alias_retains_physical_input(home, operation):
    kb.create_board('owned')
    workspace = kb.workspaces_root(board='owned') / 'Shared'
    workspace.mkdir(parents=True)
    alias = workspace.with_name('shared')
    if not alias.exists():
        pytest.skip('Temporary filesystem does not provide case-insensitive aliases')
    assert alias.samefile(workspace)
    blob = workspace / 'exact-input.txt'
    blob.write_bytes(b'EXACT native case-alias input')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='settled scratch owner', workspace_kind='scratch',
                                workspace_path=str(workspace))
        second = kb.create_task(conn, title='EXACT retained case alias', assignee='owner',
                                 workspace_kind='dir', workspace_path=str(alias))
        claimed = kb.claim_task(conn, second)
        _begin_launch(conn, claimed, str(alias), 'owned')
        if operation == 'blob':
            aid = kb.add_attachment(conn, first, filename=blob.name, stored_path=str(blob))
            assert kb.delete_attachment(conn, aid) is not None
        else:
            if operation == 'attachment':
                kb.add_attachment(conn, second, filename=blob.name, stored_path=str(alias / blob.name))
            assert kb.complete_task(conn, first, summary='EXACT result before physical settlement')
        assert (alias / blob.name).read_bytes() == b'EXACT native case-alias input'
