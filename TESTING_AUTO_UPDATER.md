# Auto-Updater Testing Guide

The auto-updater checks GitHub Releases every 30 minutes (configurable) and handles critical yt-dlp updates with fallback awareness.

## Environment Setup

Add or modify in `.env`:
```dotenv
AUTO_UPDATE=true                    # Enable auto-update checks
AUTO_UPDATE_CHECK_INTERVAL=60       # Check every 60 seconds (for testing)
```

For production: `AUTO_UPDATE_CHECK_INTERVAL=1800` (30 minutes, default).

## Test Cases

### Test 1: Normal Update Check (No Updates Available)

**Setup:**
- Set `AUTO_UPDATE_CHECK_INTERVAL=60` in `.env`
- Start OSR
- Watch logs for 2+ minutes

**Expected Result:**
- Logs show: `"Update manager initialized (v1.1.0, check interval: 60s)"`
- After ~60 seconds: `"GitHub API call" or "update check completed"` (info level, may be debug)
- No update messages if GitHub releases are the same version

**Verification:**
```bash
# Check logs for:
# [INFO] Update manager initialized (v1.1.0, check interval: 60s)
# [DEBUG] GitHub API call failed: [error] (if no internet or API issue)
```

---

### Test 2: Detect Available Normal Update

**Setup:**
1. Create a test GitHub release:
   - Tag: `v1.2.0` (higher than current `1.1.0`)
   - Name: `v1.2.0` (no `[yt-dlp-update]` keyword)
   - Body: `This is a normal update with bug fixes`

2. Temporarily modify `update_manager.py` line 37 to force check:
   ```python
   # Old:
   if self._last_check_time and (now - self._last_check_time) < self._check_interval:
   
   # Temporary (testing only):
   if False:  # Skip rate limit for testing
   ```
   (Revert after testing)

3. Set `AUTO_UPDATE_CHECK_INTERVAL=5` in `.env`

4. Start OSR and wait ~5 seconds for check

**Expected Result:**
- Logs show: `Update available: v1.2.0 (normal)`
- Discord notification with title: `📦 Update Available`
- Message mentions "restart application to install"
- No auto-restart (normal updates never auto-restart)

**Verification:**
```bash
# Logs should show:
# [INFO] Update available: v1.2.0 (normal)

# Discord notification:
# Title: 📦 Update Available
# Body: OpenStreamRotator v1.2.0 is available...
```

---

### Test 3: Detect Critical Update (Fallback Inactive)

**Setup:**
1. Create a test GitHub release tagged `[yt-dlp-update]`:
   - Tag: `v1.3.0`
   - Name: `v1.3.0 [yt-dlp-update]` or body contains `[yt-dlp-update]`

2. Force check (same as Test 2: temporarily disable rate limit)

3. Set `AUTO_UPDATE_CHECK_INTERVAL=5`

4. Start OSR with fallback **NOT active** and wait for check

**Expected Result:**
- Logs show: `Update available: v1.3.0 (CRITICAL)`
- Discord notification with title: `🔴 Critical Update Available`
- Message says: "will auto-install if fallback activates"
- **No immediate restart** (fallback not active)
- Update remains pending internally

**Verification:**
```bash
# Logs should show:
# [INFO] Update available: v1.3.0 (CRITICAL)
# [INFO] Critical update v1.3.0 available (will auto-install if fallback activates)

# Discord notification:
# Title: 🔴 Critical Update Available
# Body: ...will auto-install if stream recovery is needed
```

---

### Test 4: Critical Update Auto-Install (With Fallback Active)

**Prerequisites:**
- Test 3 passed (critical update detected)
- Fallback is currently active (trigger manually via dashboard or by causing download failures)

**Setup:**
1. After Test 3 (critical update pending), activate fallback by:
   - **Option A:** Trigger 3 consecutive download failures
   - **Option B:** Use dashboard to manually activate if UI supports it
   
2. Wait for main loop to check again (next `AUTO_UPDATE_CHECK_INTERVAL` tick)

**Expected Result:**
- Logs show: `AUTO-RESTARTING for critical update (fallback mode active)`
- Discord notification: `"Auto-restarting for critical update v1.3.0"`
- Application initiates graceful shutdown
- On restart: version updates to `v1.3.0`

**Verification:**
```bash
# Check logs:
# [ERROR] AUTO-RESTARTING for critical update (fallback mode active)
# [INFO] Fallback deactivation scheduled...

# Discord notification appears with auto-restart message
# On restart: check Version in logs or UI shows v1.3.0 (if binary was pre-updated)
```

---

### Test 5: Suppress Notifications

**Setup:**
1. Pending update exists (any type)
2. Call `update_manager.suppress_update_until(3600)` to suppress for 1 hour
   - This requires modifying `check_for_updates()` to test (not user-facing)

**Expected Result:**
- Update check runs but suppresses notifications
- Logs show: `Update notifications suppressed for 3600s`
- No Discord messages during suppression window
- After suppression expires, notifications resume

---

### Test 6: Version Comparison Edge Cases

**Setup:**
Add these test calls to `update_manager.py` (temporary):
```python
mgr = UpdateManager("1.1.0")
print(mgr._is_newer_version("1.1.1"))    # True
print(mgr._is_newer_version("1.2.0"))    # True
print(mgr._is_newer_version("2.0.0"))    # True
print(mgr._is_newer_version("1.1.0"))    # False (same)
print(mgr._is_newer_version("1.0.9"))    # False (older)
print(mgr._is_newer_version("1.1.0.1"))  # True (longer)
```

**Expected Result:**
- All comparisons return correct boolean
- Handles semantic versioning correctly

---

## Testing Checklist

- [ ] **Environment** configured with `AUTO_UPDATE=true`
- [ ] **normal.pyi check interval set to 60-120 seconds** (for testing, not production)
- [ ] **OSR starts** without errors, logs show UpdateManager initialized
- [ ] **Check for available `[yt-dlp-update]` release** on GitHub for your test
- [ ] **Discord webhook** configured to receive notifications
- [ ] **Test Case 1** passes: Normal check with no updates available
- [ ] **Test Case 2** passes: Normal update detected (no auto-restart)
- [ ] **Test Case 3** passes: Critical update detected, fallback inactive (notify only)
- [ ] **Test Case 4** passes: Critical update + fallback active (auto-restart)
- [ ] **Test Case 5** passes: Notification suppression works
- [ ] **Test Case 6** passes: Version comparisons correct
- [ ] **Rate limiting** works (doesn't spam checks after 30 min)
- [ ] **reload_env()** updates AUTO_UPDATE settings correctly

## Troubleshooting

**UpdateManager not initializing:**
- Check `AUTO_UPDATE=true` in `.env` (if false, UpdateManager won't be created)
- Check logs for: `"Update manager initialized"` message

**No Discord notifications:**
- Verify `DISCORD_WEBHOOK_URL` is set
- Check if webhook is responding (test with curl)
- Look for notification errors in logs

**Rate limit causing issues:**
- For testing, temporarily set `AUTO_UPDATE_CHECK_INTERVAL=5` (5 seconds)
- For production, use default 1800 (30 minutes)
- Don't hammer GitHub API with frequent checks

**Version comparison wrong:**
- Ensure semantic versioning format: `major.minor.patch`
- Test with: `1.0.0`, `1.1.0`, `2.0.0` format
- Non-standard versions may fail comparison

**Auto-restart not triggering:**
- Verify fallback is actually active: check obs_controller logs for fallback mode
- Check that release has `[yt-dlp-update]` in name or body (case-insensitive)
- Check logs for: `"AUTO-RESTARTING for critical update"`

## Production Deployment

Once tested, ensure:
1. **VERSION** in `config/constants.py` reflects actual release version
2. **AUTO_UPDATE_CHECK_INTERVAL** set to 1800 (30 minutes) or higher
3. **AUTO_UPDATE** set to `true` (or use environment variable to control)
4. **DISCORD_WEBHOOK_URL** configured for production notifications
5. GitHub release properly tagged with `[yt-dlp-update]` if critical

## Key Design Points

- **No auto-restart for normal updates** → User controls restart timing
- **Auto-restart only for critical updates + fallback active** → Minimizes stream interruption
- **30-minute check interval default** → Balances freshness with API load
- **Suppression window** → Prevents notification spam on repeated updates
- **Fallback awareness** → Only auto-restarts when stream recovery is more important
