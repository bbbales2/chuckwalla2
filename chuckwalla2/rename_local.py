import glob
import os
import shutil

src_dir = "chuckwalla-storage/nba/raw/games"
dst_dir = "chuckwalla-storage2/nba/raw/games"
old_names = list(glob.glob("*", root_dir=src_dir))

for name in old_names:
    date = name.split(".")[0]
    dst = os.path.join(dst_dir, f"partition_date={date}", "0000.json")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy(os.path.join(src_dir, name), dst)
    #os.rename()
