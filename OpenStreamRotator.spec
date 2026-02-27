# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec file for OpenStreamRotator."""

import os
import sys

block_cipher = None

# Collect all Python source packages in the project
a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        # Project packages
        'config',
        'config.config_manager',
        'config.constants',
        'controllers',
        'controllers.automation_controller',
        'controllers.obs_controller',
        'core',
        'core.database',
        'core.video_registration_queue',
        'handlers',
        'handlers.content_switch_handler',
        'handlers.dashboard_handler',
        'handlers.temp_playback_handler',
        'integrations',
        'integrations.platforms',
        'integrations.platforms.base',
        'integrations.platforms.base.stream_platform',
        'integrations.platforms.kick',
        'integrations.platforms.twitch',
        'lib',
        'lib.kickpython',
        'lib.kickpython.kickpython',
        'lib.kickpython.kickpython.api',
        'managers',
        'managers.download_manager',
        'managers.obs_connection_manager',
        'managers.platform_manager',
        'managers.playlist_manager',
        'managers.prepared_rotation_manager',
        'managers.rotation_manager',
        'managers.stream_manager',
        'monitors',
        'monitors.obs_freeze_monitor',
        'playback',
        'services',
        'services.kick_live_checker',
        'services.notification_service',
        'services.twitch_live_checker',
        'services.web_dashboard_client',
        'utils',
        'utils.playlist_selector',
        'utils.video_downloader',
        'utils.video_processor',
        'utils.video_utils',
        # Third-party hidden imports PyInstaller may miss
        'aiohttp',
        'curl_cffi',
        'curl_cffi.requests',
        'websockets',
        'websockets.asyncio',
        'websockets.asyncio.client',
        'obsws_python',
        'dotenv',
        'yt_dlp',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='OpenStreamRotator',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='OpenStreamRotator',
)
