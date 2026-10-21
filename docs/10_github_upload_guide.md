# 10. GitHub Upload Guide

1. Download the repository ZIP and extract the `enterprise-ev-data-platform` folder.
2. Create a new GitHub repository with a matching name.
3. Before upload, inspect `.gitignore`, ADF JSON, notebook exports, screenshots, PBIX and PDF for credentials or private identifiers.
4. Upload the extracted files, preserving the folder structure. Do not upload the ZIP inside the repository unless you intentionally want a nested archive.
5. Open the GitHub README and check that images render. Relative links should work when the full folder structure is preserved.
6. Confirm the `.pbix` is present under `reports/powerbi/` and the PDF under the same folder.
7. Keep a private backup of the editable report and notebooks.

## Optional Git command workflow
```bash
git init
git add .
git status
# Inspect staged files for secrets before committing.
git commit -m "Add Enterprise EV IoT Data Platform MVP"
git branch -M main
git remote add origin https://github.com/<your-user>/enterprise-ev-data-platform.git
git push -u origin main
```

Replace `<your-user>` with your GitHub username. Never paste tokens into source files or the remote URL.
