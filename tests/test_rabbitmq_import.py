"""Tests for RabbitMQ backend import handling when pika is not installed."""

import subprocess
import sys


def test_rabbitmq_import_error_pika_not_installed():
    """Test ImportError is raised with correct message when pika is not available.

    This test verifies that when pika is not installed, importing the RabbitMQ
    backend raises an ImportError with a helpful message about how to install it.
    """
    # Use subprocess to avoid corrupting the current process's module state
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "# Block pika from being imported\n"
                "sys.modules['pika'] = None\n"
                "sys.modules['pika.exceptions'] = None\n"
                "try:\n"
                "    import scrapy_extension.backends.rabbitmq\n"
                "    print('ERROR: No ImportError raised')\n"
                "    sys.exit(1)\n"
                "except ImportError as e:\n"
                "    msg = str(e)\n"
                '    if "pika" in msg and "scrapy-extension[rabbitmq]" in msg:\n'
                "        print('PASS')\n"
                "    else:\n"
                "        print(f'ERROR: Wrong message: {msg}')\n"
                "        sys.exit(1)\n"
            ),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"subprocess failed: {result.stderr}\n{result.stdout}"
    assert "PASS" in result.stdout


def test_rabbitmq_import_error_cause_is_original_import_error():
    """Test that the ImportError chains the original ImportError as __cause__.

    This verifies that when pika import fails, the raised ImportError properly
    chains to the original error using 'raise ... from e'.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "sys.modules['pika'] = None\n"
                "sys.modules['pika.exceptions'] = None\n"
                "try:\n"
                "    import scrapy_extension.backends.rabbitmq\n"
                "    print('ERROR: No ImportError raised')\n"
                "    sys.exit(1)\n"
                "except ImportError as e:\n"
                "    if e.__cause__ is None:\n"
                "        print('ERROR: No chained exception')\n"
                "        sys.exit(1)\n"
                "    print('PASS')\n"
            ),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"subprocess failed: {result.stderr}\n{result.stdout}"
    assert "PASS" in result.stdout
