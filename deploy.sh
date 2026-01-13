#!/bin/bash
set -e

SERVER="lcdelevadmin03@lcd-apps"
APP_DIR="/home/lcdelevadmin03/apps/metabase"
SERVICES=( )
REMOTE_PATH="/home/lcdelevadmin03/apps/metabase"

# 1. Push local changes to GitHub
echo "📤 Pushing local changes to GitHub..."
git add .
git commit -m "Local changes" || true
git push origin main

# 2. Ask if you want to update server files
read -p "Do you want to update server files from Git? (y/n): " UPDATE_SERVER
if [[ "$UPDATE_SERVER" == "y" ]]; then
    echo "🔄 Updating server repo..."
    ssh $SERVER "cd $APP_DIR && git fetch origin && git reset --hard origin/main"
    
    # 3. Copy git-ignored files
    echo "📂 Copying ignored files..."
        FILES_TO_COPY=(
            "config/.env"
            )

            for f in "${FILES_TO_COPY[@]}"; do
                if [ -d "$f" ]; then
                    echo "📂 Copying directory $f"
                    # Ensure the full target directory exists (not just the parent)
                    ssh "$SERVER" "mkdir -p '$REMOTE_PATH/$f'"
                    scp -r "$f" "$SERVER:$REMOTE_PATH/$(dirname "$f")/"
                elif [ -f "$f" ]; then
                    echo "📄 Copying file $f"
                    ssh "$SERVER" "mkdir -p '$REMOTE_PATH/$(dirname "$f")'"
                    scp "$f" "$SERVER:$REMOTE_PATH/$f"
                else
                    echo "⚠️ Skipping missing path: $f"
                fi
                done
fi

# 4. Ask whether to restart any services at all
read -p "Do you want to restart any services? (y/n): " RESTART_ANY
if [[ "$RESTART_ANY" == "y" ]]; then
    echo "⚙️  Service restart options:"
    for i in "${!SERVICES[@]}"; do
        printf "  %2d) %s\n" "$((i + 1))" "${SERVICES[$i]}"
    done

    read -p "Enter service numbers to restart (space/comma separated, blank to skip): " SERVICE_SELECTION
    SERVICE_SELECTION=${SERVICE_SELECTION//,/ }
    if [[ -z "${SERVICE_SELECTION// }" ]]; then
        echo "⏩ No services selected."
    else
        declare -A RESTARTED=()
        for selection in $SERVICE_SELECTION; do
            if [[ ! "$selection" =~ ^[0-9]+$ ]]; then
                echo "⚠️  Skipping invalid entry: $selection"
                continue
            fi
            idx=$((selection - 1))
            if (( idx < 0 || idx >= ${#SERVICES[@]} )); then
                echo "⚠️  Skipping out-of-range selection: $selection"
                continue
            fi
            SERVICE="${SERVICES[$idx]}"
            if [[ -n "${RESTARTED[$SERVICE]}" ]]; then
                echo "↻ Already restarted $SERVICE (skipping duplicate selection)"
                continue
            fi
            # Allocate a TTY so sudo can prompt for a password if needed
            ssh -tt $SERVER "sudo systemctl restart $SERVICE"
            RESTARTED[$SERVICE]=1
            echo "✅ Restarted $SERVICE"
        done
    fi
else
    echo "🛑 Service restarts skipped."
fi
