# GitHub Upload Instructions

**Status:** Git repository initialized and committed locally. Authentication needed to push.

**Error encountered:**
```
remote: Permission to nithyas-lab/NABCA.git denied to nithyashreeraman.
fatal: The requested URL returned error: 403
```

---

## Option 1: Using Personal Access Token (PAT) - RECOMMENDED

### Step 1: Create a GitHub Personal Access Token

1. Go to: https://github.com/settings/tokens
2. Click **"Generate new token"** → **"Generate new token (classic)"**
3. Give it a name: `NABCA Upload Token`
4. Set expiration: Choose your preference (30 days, 60 days, or No expiration)
5. Select scopes:
   - ✅ **repo** (Full control of private repositories)
6. Click **"Generate token"**
7. **COPY THE TOKEN** - you won't see it again!

### Step 2: Update Git Remote with Token

Open Command Prompt in `GITHUB_CODES` folder and run:

```bash
cd C:\Users\nithy\OneDrive\Desktop\Twenty20\NABCA\GITHUB_CODES

# Remove old remote
git remote remove origin

# Add new remote with token (replace YOUR_TOKEN_HERE with actual token)
git remote add origin https://YOUR_TOKEN_HERE@github.com/nithyas-lab/NABCA.git

# Push to GitHub
git push -u origin master
```

**Example:**
If your token is `ghp_abc123xyz789`, the command would be:
```bash
git remote add origin https://ghp_abc123xyz789@github.com/nithyas-lab/NABCA.git
```

---

## Option 2: Check Repository Permissions

The error might be because `nithyashreeraman` doesn't have write access to `nithyas-lab/NABCA`.

### If you own the repository:
1. Go to: https://github.com/nithyas-lab/NABCA
2. Verify you're logged in as the correct user
3. Check Settings → Collaborators & teams
4. Make sure your account has write access

### If the repository doesn't exist yet:
Create it first:
1. Go to: https://github.com/new
2. Owner: Select `nithyas-lab`
3. Repository name: `NABCA`
4. Make it Private or Public (your choice)
5. Don't initialize with README (we already have files)
6. Click **"Create repository"**
7. Then use Option 1 above to push

---

## Option 3: Using GitHub CLI (if installed)

If you have GitHub CLI installed:

```bash
cd C:\Users\nithy\OneDrive\Desktop\Twenty20\NABCA\GITHUB_CODES

# Authenticate
gh auth login

# Push
git push -u origin master
```

---

## What Will Be Uploaded

**19 files ready to push:**

### Extraction Scripts (8 files)
- brand_leaders.py
- brand_summary.py
- current_month.py
- rolling_12m.py
- top100_vendors.py
- top20_by_class.py
- vendor_summary.py
- ytd.py

### Validation Scripts (4 files)
- validation_scripts/verify_all_systems.py
- validation_scripts/comprehensive_data_quality_scan.py
- validation_scripts/vendor_summary_monthly_split.py
- validation_scripts/vendor_summary_accuracy_excluding_totals.py

### Cleanup Scripts (3 files)
- cleanup_scripts/remove_total_vendor_rows.py
- cleanup_scripts/fix_merged_total_vendor.py
- cleanup_scripts/fix_duplicated_class_names.py

### Documentation (4 files)
- README.md
- QUICK_START_GUIDE.md
- EXTRACTION_LOGIC_DOCUMENTATION.md
- FILES_READY_TO_SHARE.md

---

## After Successful Push

You should see:
```
Enumerating objects: 24, done.
Counting objects: 100% (24/24), done.
Delta compression using up to X threads
Compressing objects: 100% (23/23), done.
Writing objects: 100% (24/24), XXX KB | XXX MB/s, done.
Total 24 (delta 0), reused 0 (delta 0)
To https://github.com/nithyas-lab/NABCA.git
 * [new branch]      master -> master
Branch 'master' set up to track remote branch 'master' from 'origin'.
```

Then visit: https://github.com/nithyas-lab/NABCA to see your uploaded code!

---

## Troubleshooting

**Error: "repository not found"**
- The repository doesn't exist yet. Create it first (see Option 2)

**Error: "authentication failed"**
- Your token expired or is incorrect
- Generate a new token and try again

**Error: "permission denied"**
- You don't have write access to the repository
- Make sure you're logged in as the repository owner

---

**Need help?** Let me know which option you want to use, and I can provide more specific guidance.
