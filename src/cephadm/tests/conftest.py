"""pytest configuration for the cephadm test suite.

FreeBSD-specific workarounds for pyfakefs 5.3.5 / CPython 3.12
interactions. Neither of these reflects a real bug in cephadm; both
are pinned-dependency quirks that only surface on FreeBSD.
"""
import sys
import shutil

if sys.platform.startswith('freebsd'):
    # CPython's shutil.rmtree() decides at import time (module load of
    # shutil, before pyfakefs patches os) whether to use the fd-based
    # "safe" walk (_rmtree_safe_fd), based on whether the *real* os
    # module reports dir_fd support for open/stat/unlink/rmdir.
    # FreeBSD reports this support, so _use_fd_functions ends up True.
    # Under pyfakefs 5.3.5 the patched os.lstat returned during that
    # fd-based walk does not preserve object identity across calls,
    # which trips the "assert func is os.lstat" in
    # shutil._rmtree_safe_fd() -- entirely inside stdlib/pyfakefs,
    # unrelated to cephadm logic. Forcing the classic path-based
    # rmtree walk avoids the assert and pyfakefs handles that path
    # correctly.
    if hasattr(shutil, '_use_fd_functions'):
        shutil._use_fd_functions = False


def pytest_configure(config):
    if not sys.platform.startswith('freebsd'):
        return
    # pyfakefs 5.3.5's TemporaryFileCloser patching for Python 3.12
    # doesn't always match CPython's GC finalization order: __del__
    # can fire after pyfakefs has already unpatched for that test,
    # hitting an already-closed backing BytesIO. Harmless -- the
    # actual test outcome is unaffected -- but noisy. Silenced rather
    # than "fixed", since a real fix means patching pyfakefs's
    # tempfile finalizer internals.
    #
    # Registered via pytest's own filterwarnings ini mechanism rather
    # than a plain warnings.filterwarnings() call: pytest wraps every
    # test in warnings.catch_warnings() + simplefilter("always"),
    # which discards any filter added at plain import/conftest time
    # before the first test even runs. addinivalue_line() re-applies
    # the filter for every test item instead.
    config.addinivalue_line(
        'filterwarnings',
        'ignore:Exception ignored in.*_TemporaryFileCloser.*:'
        'pytest.PytestUnraisableExceptionWarning',
    )

