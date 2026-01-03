"""Windows Service support for Fractal Connector."""
import sys
import os
import time
import logging
import asyncio
from pathlib import Path

logger = logging.getLogger(__name__)

# Check if we're on Windows
IS_WINDOWS = sys.platform == 'win32'

if IS_WINDOWS:
    try:
        import win32serviceutil
        import win32service
        import win32event
        import servicemanager
        HAS_PYWIN32 = True
    except ImportError:
        HAS_PYWIN32 = False
else:
    HAS_PYWIN32 = False


class FractalConnectorService:
    """
    Windows Service wrapper for Fractal Connector.

    Install:   python -m src.core.service install
    Start:     python -m src.core.service start
    Stop:      python -m src.core.service stop
    Remove:    python -m src.core.service remove
    """

    _svc_name_ = "FractalConnector"
    _svc_display_name_ = "Fractal Connector Service"
    _svc_description_ = "Universal data connector for Fractal platform"

    def __init__(self):
        self.connector = None
        self.loop = None
        self._running = False

    def start(self):
        """Start the service."""
        logger.info("Starting Fractal Connector Service...")
        self._running = True

        # Import here to avoid circular imports
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from main import FractalConnector

        self.connector = FractalConnector()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        try:
            self.loop.run_until_complete(self.connector.start(open_browser=False))
            logger.info("Fractal Connector Service started")

            # Keep running until stopped
            while self._running:
                self.loop.run_until_complete(asyncio.sleep(1))

        except Exception as e:
            logger.error(f"Service error: {e}")
        finally:
            if self.connector:
                self.loop.run_until_complete(self.connector.stop())
            self.loop.close()

    def stop(self):
        """Stop the service."""
        logger.info("Stopping Fractal Connector Service...")
        self._running = False


if IS_WINDOWS and HAS_PYWIN32:
    class WindowsService(win32serviceutil.ServiceFramework):
        """Windows Service implementation using pywin32."""

        _svc_name_ = FractalConnectorService._svc_name_
        _svc_display_name_ = FractalConnectorService._svc_display_name_
        _svc_description_ = FractalConnectorService._svc_description_

        def __init__(self, args):
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.stop_event = win32event.CreateEvent(None, 0, 0, None)
            self.service = FractalConnectorService()

        def SvcStop(self):
            """Called when the service is asked to stop."""
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            self.service.stop()
            win32event.SetEvent(self.stop_event)

        def SvcDoRun(self):
            """Called when the service is asked to start."""
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, '')
            )
            self.service.start()


def install_service():
    """Install the Windows service."""
    if not IS_WINDOWS:
        print("Windows service installation is only available on Windows.")
        return False

    if not HAS_PYWIN32:
        print("pywin32 is required for Windows service support.")
        print("Install with: pip install pywin32")
        return False

    try:
        # Get the path to the Python executable and script
        python_exe = sys.executable
        script_path = os.path.abspath(__file__)

        # Install the service
        win32serviceutil.InstallService(
            None,
            FractalConnectorService._svc_name_,
            FractalConnectorService._svc_display_name_,
            startType=win32service.SERVICE_AUTO_START,
            description=FractalConnectorService._svc_description_,
        )
        print(f"Service '{FractalConnectorService._svc_name_}' installed successfully.")
        return True

    except Exception as e:
        print(f"Failed to install service: {e}")
        return False


def uninstall_service():
    """Uninstall the Windows service."""
    if not IS_WINDOWS or not HAS_PYWIN32:
        print("Windows service management is only available on Windows with pywin32.")
        return False

    try:
        win32serviceutil.RemoveService(FractalConnectorService._svc_name_)
        print(f"Service '{FractalConnectorService._svc_name_}' removed successfully.")
        return True
    except Exception as e:
        print(f"Failed to remove service: {e}")
        return False


def start_service():
    """Start the Windows service."""
    if not IS_WINDOWS or not HAS_PYWIN32:
        return False

    try:
        win32serviceutil.StartService(FractalConnectorService._svc_name_)
        print(f"Service '{FractalConnectorService._svc_name_}' started.")
        return True
    except Exception as e:
        print(f"Failed to start service: {e}")
        return False


def stop_service():
    """Stop the Windows service."""
    if not IS_WINDOWS or not HAS_PYWIN32:
        return False

    try:
        win32serviceutil.StopService(FractalConnectorService._svc_name_)
        print(f"Service '{FractalConnectorService._svc_name_}' stopped.")
        return True
    except Exception as e:
        print(f"Failed to stop service: {e}")
        return False


if __name__ == '__main__':
    if IS_WINDOWS and HAS_PYWIN32:
        if len(sys.argv) == 1:
            # Called without arguments - run as service
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(WindowsService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
            # Handle command line arguments
            win32serviceutil.HandleCommandLine(WindowsService)
    else:
        print("Windows service support requires Windows and pywin32.")
        print("For non-Windows systems, use systemd or another service manager.")
