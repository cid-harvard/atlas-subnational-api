set -e
echo "If using a glob pattern like * you might want to put it in single quotes."
echo
find $1
read -p "Going to gzip / rename these, does it look okay? (Ctrl-C to cancel)"
find $1 -exec gzip -9 "{}" \; -exec mv "{}.gz" {} \;
find $1 -exec file "{}" \;
