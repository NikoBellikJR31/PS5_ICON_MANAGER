from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse
from starlette.middleware.cors import CORSMiddleware
import os
import logging
from pathlib import Path
from pydantic import BaseModel
import uuid
import aioftp
import aiosqlite
from PIL import Image
import io
import base64
import tempfile
import json
import asyncio
import struct
import shutil
from datetime import datetime
import traceback

ROOT_DIR = Path(__file__).parent
CACHE_DIR = ROOT_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

app = FastAPI()
api_router = APIRouter(prefix="/api")
ftp_connections = {}
user_home_cache = {}  # session_id -> user_home_id

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

IP_HISTORY_FILE = ROOT_DIR / "ip_history.json"

SIZES = {
    "icon0": (512, 512),
    "pic0": (1920, 1080),
    "pic1": (1920, 1080),
    "pic2": (1920, 1080),
    "save_data": (688, 388),
}

def load_ip_history():
    try:
        if IP_HISTORY_FILE.exists():
            with open(IP_HISTORY_FILE, 'r') as f:
                return json.load(f)
    except:
        pass
    return []

def save_ip_history(ip, port):
    history = load_ip_history()
    entry = f"{ip}:{port}"
    if entry in history:
        history.remove(entry)
    history.insert(0, entry)
    history = history[:5]
    with open(IP_HISTORY_FILE, 'w') as f:
        json.dump(history, f)
    return history

def dds_to_png_base64(dds_data):
    try:
        img = Image.open(io.BytesIO(dds_data))
        out = io.BytesIO()
        img.save(out, format='PNG')
        out.seek(0)
        return base64.b64encode(out.read()).decode('utf-8')
    except:
        return None

def png_to_dds_bytes(img):
    w, h = img.size
    if img.mode != 'RGBA':
        img = img.convert('RGBA')
    pixels = img.tobytes()
    bgra = bytearray(len(pixels))
    for i in range(0, len(pixels), 4):
        bgra[i] = pixels[i+2]
        bgra[i+1] = pixels[i+1]
        bgra[i+2] = pixels[i]
        bgra[i+3] = pixels[i+3]
    header = bytearray(128)
    header[0:4] = b'DDS '
    struct.pack_into('<I', header, 4, 124)
    struct.pack_into('<I', header, 8, 0x1 | 0x2 | 0x4 | 0x8 | 0x1000)
    struct.pack_into('<I', header, 12, h)
    struct.pack_into('<I', header, 16, w)
    struct.pack_into('<I', header, 20, w * 4)
    struct.pack_into('<I', header, 76, 32)
    struct.pack_into('<I', header, 80, 0x41)
    struct.pack_into('<I', header, 88, 32)
    struct.pack_into('<I', header, 92, 0x00FF0000)
    struct.pack_into('<I', header, 96, 0x0000FF00)
    struct.pack_into('<I', header, 100, 0x000000FF)
    struct.pack_into('<I', header, 104, 0xFF000000)
    struct.pack_into('<I', header, 108, 0x1000)
    return bytes(header) + bytes(bgra)

async def fresh_ftp(session_id):
    if session_id not in ftp_connections:
        return None
    conn = ftp_connections[session_id]
    try:
        await conn["client"].quit()
    except:
        pass
    try:
        ftp = aioftp.Client()
        await ftp.connect(conn["ip"], conn["port"])
        ftp_connections[session_id]["client"] = ftp
        return ftp
    except:
        return None

async def get_ftp(session_id):
    if session_id not in ftp_connections:
        return None
    ftp = ftp_connections[session_id]["client"]
    try:
        await asyncio.wait_for(ftp.list("/"), timeout=3)
        return ftp
    except:
        return await fresh_ftp(session_id)

async def ftp_download(ftp, path, retries=3, delay=0.5):
    for attempt in range(retries):
        try:
            stream = io.BytesIO()
            async with ftp.download_stream(path) as s:
                async for block in s.iter_by_block():
                    stream.write(block)
            stream.seek(0)
            data = stream.read()
            if len(data) > 0:
                return data
            raise Exception("Empty")
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(delay * (attempt + 1))
            else:
                raise e

async def ftp_upload(ftp, data, path, retries=3, delay=0.5):
    for attempt in range(retries):
        try:
            async with ftp.upload_stream(path) as s:
                await s.write(data)
            return True
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(delay * (attempt + 1))
            else:
                raise e

async def ftp_list_dir(ftp, path):
    files = []
    try:
        async for item_path, info in ftp.list(path):
            files.append({"name": item_path.name, "type": info.get("type", "file")})
    except:
        pass
    return files

def get_cached_db():
    """Get path to cached app.db"""
    p = CACHE_DIR / "app.db"
    if p.exists():
        return str(p)
    return None

async def query_db_for_app(app_id):
    """Query CACHED local DB for an app - no FTP needed"""
    db_path = get_cached_db()
    if not db_path:
        return {"entries": [], "concept_ids": [], "paths": {}}
    
    entries = []
    concept_ids = set()
    paths = {}
    
    try:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in await cursor.fetchall()]
            
            for table in tables:
                try:
                    cursor = await db.execute(f"PRAGMA table_info({table})")
                    col_names = [c[1] for c in await cursor.fetchall()]
                    
                    found = False
                    for col in col_names:
                        if found:
                            break
                        try:
                            cursor = await db.execute(
                                f"SELECT * FROM {table} WHERE CAST({col} AS TEXT) LIKE ?",
                                (f"%{app_id}%",)
                            )
                            rows = await cursor.fetchall()
                            if rows:
                                found = True
                                descs = [d[0] for d in cursor.description]
                                for row in rows:
                                    rd = dict(zip(descs, row))
                                    
                                    for cid_col in ["conceptId", "localConceptId"]:
                                        if cid_col in rd and rd[cid_col]:
                                            val = str(rd[cid_col])
                                            for part in val.replace(':', ' ').replace('-', ' ').split():
                                                if part.isdigit() and len(part) >= 4:
                                                    concept_ids.add(part)
                                    
                                    for k, v in rd.items():
                                        if v and isinstance(v, str) and '/' in v and not v.startswith('http'):
                                            paths[f"{table}.{k}"] = v
                                    
                                    entries.append({
                                        "db_file": "app.db (cache local)",
                                        "table": table,
                                        "data": {k: str(v) if v is not None else None for k, v in rd.items()},
                                    })
                        except:
                            continue
                except:
                    continue
    except Exception as e:
        logger.error(f"Query DB error: {e}")
    
    return {"entries": entries, "concept_ids": list(concept_ids), "paths": paths}

class FTPConnection(BaseModel):
    ip: str
    port: int = 2121

@api_router.get("/")
async def root():
    return {"message": "PS5 Icon Manager API"}

@api_router.get("/ip-history")
async def get_ip_history():
    return {"history": load_ip_history()}

@api_router.post("/ftp/connect")
async def connect_ftp(connection: FTPConnection):
    try:
        session_id = str(uuid.uuid4())
        ftp_client = aioftp.Client()
        await ftp_client.connect(connection.ip, connection.port)
        ftp_connections[session_id] = {"client": ftp_client, "ip": connection.ip, "port": connection.port}
        history = save_ip_history(connection.ip, connection.port)
        
        # Download app.db to cache on connect
        db_status = "non telechargee"
        try:
            logger.info("Downloading app.db to cache...")
            db_data = await ftp_download(ftp_client, "/system_data/priv/mms/app.db", retries=3, delay=1.0)
            if db_data:
                db_file = CACHE_DIR / "app.db"
                with open(db_file, 'wb') as f:
                    f.write(db_data)
                db_status = f"OK ({len(db_data)} bytes)"
                logger.info(f"Cached app.db: {len(db_data)} bytes")
        except Exception as e:
            db_status = f"erreur: {e}"
            logger.warning(f"Could not cache app.db: {e}")
        
        # Reconnect after DB download (FTP might have died)
        try:
            ftp_client = aioftp.Client()
            await ftp_client.connect(connection.ip, connection.port)
            ftp_connections[session_id]["client"] = ftp_client
        except:
            pass
        
        return {"success": True, "session_id": session_id, "ip_history": history, "db_cache": db_status}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/refresh-db")
async def refresh_db(session_id: str):
    """Re-download app.db to cache"""
    ftp = await fresh_ftp(session_id)
    if not ftp:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    try:
        db_data = await ftp_download(ftp, "/system_data/priv/mms/app.db", retries=3, delay=1.0)
        if db_data:
            with open(CACHE_DIR / "app.db", 'wb') as f:
                f.write(db_data)
            # Reconnect
            await fresh_ftp(session_id)
            return {"success": True, "size": len(db_data)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/disconnect")
async def disconnect_ftp(session_id: str):
    if session_id in ftp_connections:
        try:
            await ftp_connections[session_id]["client"].quit()
        except:
            pass
        del ftp_connections[session_id]
    return {"success": True}

@api_router.get("/ftp/apps")
async def list_apps(session_id: str):
    ftp = await get_ftp(session_id)
    if not ftp:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    apps = []
    try:
        async for path, info in ftp.list("/user/app/"):
            if info["type"] == "dir":
                apps.append({"cusa_id": path.name, "name": path.name})
    except:
        pass
    return {"apps": apps}

@api_router.get("/ftp/scan-app/{app_id}")
async def scan_app(session_id: str, app_id: str):
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    try:
        logger.info(f"=== SCAN {app_id} ===")
        
        # Step 1: Query LOCAL cached DB (instant, no FTP!)
        db_scan = await query_db_for_app(app_id)
        concept_ids = db_scan["concept_ids"]
        logger.info(f"ConceptIds from cache: {concept_ids}")
        
        # Step 2: Build dirs to scan (standard locations only)
        dirs = [f"/user/appmeta/{app_id}", f"/user/app/{app_id}/sce_sys"]
        for cid in concept_ids:
            dirs.append(f"/user/catalog_downloader/conceptmeta/{cid}")
            dirs.append(f"/user/catalog_downloader/conceptmeta_v2/{cid}")
        
        for col, val in db_scan["paths"].items():
            if '/' in val:
                parts = val.rsplit('/', 1)
                d = parts[0] if len(parts) == 2 and '.' in parts[1] else val
                if d and d not in dirs:
                    dirs.append(d)
        
        dirs = list(dict.fromkeys(dirs))
        logger.info(f"Dirs: {dirs}")
        
        # Step 3: Fresh FTP for directory scanning
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP reconnect failed"})
        
        all_found = []
        for dir_path in dirs:
            files = await ftp_list_dir(ftp, dir_path)
            
            img_files = [f for f in files if f["name"].lower().endswith(('.png', '.dds', '.jpg', '.jpeg', '.webp', '.bmp')) and 
                        any(f["name"].lower().startswith(t) for t in ["icon0", "pic0", "pic1", "pic2", "save_data"])]
            
            # Only download first PNG or DDS for preview, list the rest
            preview_done = {}
            for f in img_files:
                full_path = f"{dir_path}/{f['name']}"
                fname = f["name"].lower()
                img_type = None
                for t in ["icon0", "pic0", "pic1", "pic2", "save_data"]:
                    if fname.startswith(t):
                        img_type = t
                        break
                
                b64 = None
                size = 0
                need_preview = img_type and img_type not in preview_done
                
                if need_preview:
                    try:
                        img_data = await ftp_download(ftp, full_path, retries=3, delay=0.5)
                        if img_data:
                            size = len(img_data)
                            if fname.endswith('.dds'):
                                b64 = dds_to_png_base64(img_data)
                            else:
                                b64 = base64.b64encode(img_data).decode('utf-8')
                            preview_done[img_type] = True
                    except:
                        try:
                            ftp = await fresh_ftp(session_id)
                        except:
                            pass
                
                all_found.append({
                    "dir": dir_path, "file": f["name"],
                    "full_path": full_path, "size": size, "data": b64,
                })
        
        # Group
        grouped = {}
        for img in all_found:
            fname = img["file"].lower()
            for t in ["icon0", "pic0", "pic1", "pic2", "save_data"]:
                if fname.startswith(t):
                    grouped.setdefault(t, []).append(img)
                    break
        
        images = []
        for img_type in ["icon0", "pic0", "pic1", "pic2", "save_data"]:
            locs = grouped.get(img_type, [])
            
            # Second pass: retry preview if first pass failed
            has_preview = any(loc.get("data") for loc in locs)
            if not has_preview and locs:
                try:
                    ftp = await fresh_ftp(session_id)
                    if ftp:
                        for loc in locs:
                            try:
                                img_data = await ftp_download(ftp, loc["full_path"], retries=2, delay=0.5)
                                if img_data:
                                    loc["size"] = len(img_data)
                                    fname = loc["file"].lower()
                                    if fname.endswith('.dds'):
                                        loc["data"] = dds_to_png_base64(img_data)
                                    else:
                                        loc["data"] = base64.b64encode(img_data).decode('utf-8')
                                    break
                            except:
                                pass
                except:
                    pass
            
            w, h = SIZES.get(img_type, (512, 512))
            label = {"icon0": "Jaquette", "pic0": "Background XMB", "pic1": "BG Start 1", "pic2": "BG Start 2", "save_data": "Save Data"}.get(img_type, img_type)
            images.append({"type": img_type, "label": label, "width": w, "height": h, "square": img_type == "icon0", "locations": locs})
        
        return {"success": True, "app_id": app_id, "concept_ids": concept_ids, "images": images, "db_paths": db_scan["paths"], "dirs_scanned": dirs}
    
    except Exception as e:
        logger.error(f"Scan: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/replace-image/{app_id}")
async def replace_image(session_id: str, app_id: str, image_type: str = Form(...), target_paths: str = Form(...), file: UploadFile = File(...)):
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    try:
        steps = []
        w, h = SIZES.get(image_type, (512, 512))
        
        data = await file.read()
        img = Image.open(io.BytesIO(data))
        if img.mode != 'RGBA':
            img = img.convert('RGBA')
        img = img.resize((w, h), Image.Resampling.LANCZOS)
        steps.append(f"Redimensionne {w}x{h}")
        
        png_buf = io.BytesIO()
        img.save(png_buf, format="PNG")
        png_bytes = png_buf.getvalue()
        dds_bytes = png_to_dds_bytes(img)
        
        paths = [p.strip() for p in target_paths.split(',') if p.strip()]
        
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        replaced = 0
        for target_path in paths:
            try:
                is_dds = target_path.endswith('.dds')
                file_bytes = dds_bytes if is_dds else png_bytes
                
                # Backup
                try:
                    old = await ftp_download(ftp, target_path, retries=1, delay=0.2)
                    if old:
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        await ftp_upload(ftp, old, f"{target_path}.bak_{ts}")
                        steps.append(f"Backup {target_path.split('/')[-1]}")
                except:
                    pass
                
                await ftp_upload(ftp, file_bytes, target_path)
                steps.append(f"Remplace {target_path.split('/')[-1]}")
                replaced += 1
                
                try:
                    await ftp.change_permissions(target_path, 0o444)
                except:
                    pass
                
            except Exception as e:
                steps.append(f"Erreur {target_path.split('/')[-1]}: {e}")
                try:
                    ftp = await fresh_ftp(session_id)
                except:
                    break
        
        # Also copy to sce_sys
        sce_sys_path = f"/user/app/{app_id}/sce_sys"
        try:
            await ftp.make_directory(sce_sys_path)
        except:
            pass
        for ext, fb in [(".png", png_bytes), (".dds", dds_bytes)]:
            sce_target = f"{sce_sys_path}/{image_type}{ext}"
            try:
                await ftp_upload(ftp, fb, sce_target)
                steps.append(f"+ sce_sys: {image_type}{ext}")
                replaced += 1
                try:
                    await ftp.change_permissions(sce_target, 0o444)
                except:
                    pass
            except:
                try:
                    ftp = await fresh_ftp(session_id)
                except:
                    pass
        
        steps.append(f"Total: {replaced} fichiers")
        return {"success": True, "steps": steps}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/delete-image/{app_id}")
async def delete_image(session_id: str, app_id: str, image_type: str = Form(...), paths: str = Form("")):
    """Delete image files from PS5 (all variants: png, dds, jpg)"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        steps = []
        deleted = 0
        
        # Collect all paths to delete
        all_targets = set()
        
        if paths:
            for p in paths.split(','):
                p = p.strip()
                if p:
                    all_targets.add(p)
        
        # Standard locations
        locations = [f"/user/appmeta/{app_id}", f"/user/app/{app_id}/sce_sys"]
        for loc in locations:
            for ext in [".png", ".dds", ".jpg", ".jpeg"]:
                all_targets.add(f"{loc}/{image_type}{ext}")
        
        # Delete all in one pass
        for target in all_targets:
            try:
                await ftp.remove(target)
                deleted += 1
                steps.append(f"Supprime: {target.split('/')[-1]}")
            except:
                pass
        
        # Delete backups
        for loc in locations:
            try:
                ftp = await fresh_ftp(session_id)
                async for path, info in ftp.list(loc):
                    fname = str(path).split('/')[-1]
                    if fname.startswith(image_type) and '.bak_' in fname:
                        try:
                            await ftp.remove(f"{loc}/{fname}")
                            deleted += 1
                            steps.append(f"Backup: {fname}")
                        except:
                            pass
            except:
                pass
        
        # Clean DB entry for this image type
        db_path = get_cached_db()
        if db_path:
            try:
                col_map = {"pic0": "pic0Info", "icon0": "icon0Info", "pic1": "pic1Info", "pic2": "pic2Info"}
                target_col = col_map.get(image_type)
                if target_col:
                    async with aiosqlite.connect(db_path) as db:
                        cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                        tables = [r[0] for r in await cursor.fetchall()]
                        for table in tables:
                            if "contentinfo" not in table.lower() or "concept" in table.lower():
                                continue
                            try:
                                cursor = await db.execute(f"PRAGMA table_info({table})")
                                cols = [c[1] for c in await cursor.fetchall()]
                                if target_col in cols:
                                    await db.execute(
                                        f"UPDATE {table} SET {target_col} = NULL WHERE CAST(icon0Info AS TEXT) LIKE ? OR CAST(metaDataPath AS TEXT) LIKE ?",
                                        (f"%{app_id}%", f"%{app_id}%")
                                    )
                                    steps.append(f"DB: {target_col} = NULL")
                            except:
                                pass
                        await db.commit()
                    
                    # Re-upload DB
                    ftp = await fresh_ftp(session_id)
                    if ftp:
                        with open(db_path, 'rb') as f:
                            db_data = f.read()
                        await ftp_upload(ftp, db_data, "/system_data/priv/mms/app.db", retries=3, delay=1.0)
                        steps.append("app.db nettoyee et uploadee")
            except:
                pass
        
        steps.append(f"Total: {deleted} fichier(s) supprime(s)")
        return {"success": True, "steps": steps}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/add-image/{app_id}")
async def add_new_image(session_id: str, app_id: str, image_type: str = Form(...), file: UploadFile = File(...)):
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        steps = []
        w, h = SIZES.get(image_type, (512, 512))
        data = await file.read()
        img = Image.open(io.BytesIO(data))
        if img.mode != 'RGBA':
            img = img.convert('RGBA')
        img = img.resize((w, h), Image.Resampling.LANCZOS)
        png_buf = io.BytesIO()
        img.save(png_buf, format="PNG")
        png_bytes = png_buf.getvalue()
        dds_bytes = png_to_dds_bytes(img)
        
        # Upload to BOTH locations:
        # 1. /user/appmeta/{app_id}/ (standard location)
        # 2. /user/app/{app_id}/sce_sys/ (where PS5 reads backgrounds for homebrews/games)
        upload_dirs = [
            f"/user/appmeta/{app_id}",
            f"/user/app/{app_id}/sce_sys",
        ]
        
        for base_path in upload_dirs:
            try:
                await ftp.make_directory(base_path)
            except:
                pass
            for ext, fb in [(".png", png_bytes), (".dds", dds_bytes)]:
                tp = f"{base_path}/{image_type}{ext}"
                try:
                    await ftp_upload(ftp, fb, tp)
                    steps.append(f"Upload {tp} ({len(fb)}b)")
                    try:
                        await ftp.change_permissions(tp, 0o444)
                    except:
                        pass
                except Exception as e:
                    steps.append(f"Skip {tp}: {e}")
                await asyncio.sleep(0.3)
        
        return {"success": True, "steps": steps}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.get("/ftp/db-inspect/{app_id}")
async def inspect_db(session_id: str, app_id: str):
    try:
        db_scan = await query_db_for_app(app_id)
        return {"success": True, "concept_ids": db_scan["concept_ids"], "entries": db_scan["entries"], "paths": db_scan["paths"]}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/force-db-path/{app_id}")
async def force_db_path(session_id: str, app_id: str, image_type: str = Form(...)):
    """Force a pic0/icon0 path into app.db - CREATE column if missing, then UPDATE"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    db_path = get_cached_db()
    if not db_path:
        return JSONResponse(status_code=400, content={"success": False, "detail": "DB pas en cache - reconnectez-vous"})
    
    steps = []
    ts = int(datetime.now().timestamp())
    
    # PS5 reads pic0 as DDS, icon0 as PNG
    new_dds_path = f"/user/appmeta/{app_id}/{image_type}.dds?ts={ts}"
    new_png_path = f"/user/appmeta/{app_id}/{image_type}.png?ts={ts}"
    
    # Columns to force for each image type (DDS for backgrounds, PNG for icons)
    col_targets = {
        "pic0": [("pic0Info", new_dds_path)],
        "icon0": [("icon0Info", new_png_path)],
        "pic1": [("pic1Info", new_dds_path)],
        "pic2": [("pic2Info", new_dds_path)],
    }
    
    target_updates = col_targets.get(image_type, [(f"{image_type}Info", new_png_path)])
    
    try:
        backup_db = str(CACHE_DIR / f"app.db.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        shutil.copy2(db_path, backup_db)
        steps.append(f"Backup DB: {backup_db.split('/')[-1]}")
        
        updated = 0
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in await cursor.fetchall()]
            
            # MUST target tbl_contentinfo specifically - that's the ONLY table PS5 reads for XMB backgrounds
            # Other tables (tbl_concepticoninfo, tbl_info) are ignored by PS5 for this purpose
            target_table = None
            where_col = None
            
            # First pass: find tbl_contentinfo by exact name pattern
            for table in tables:
                if "contentinfo" in table.lower() and "concept" not in table.lower():
                    try:
                        cursor = await db.execute(f"PRAGMA table_info({table})")
                        cols = [c[1] for c in await cursor.fetchall()]
                        
                        if "icon0Info" in cols:
                            cursor = await db.execute(
                                f"SELECT COUNT(*) FROM {table} WHERE icon0Info LIKE ?",
                                (f"%{app_id}%",)
                            )
                            count = (await cursor.fetchone())[0]
                            if count > 0:
                                target_table = table
                                where_col = "icon0Info"
                                steps.append(f">>> tbl_contentinfo: {target_table}")
                                break
                        
                        if "metaDataPath" in cols:
                            cursor = await db.execute(
                                f"SELECT COUNT(*) FROM {table} WHERE metaDataPath LIKE ?",
                                (f"%{app_id}%",)
                            )
                            count = (await cursor.fetchone())[0]
                            if count > 0:
                                target_table = table
                                where_col = "metaDataPath"
                                steps.append(f">>> tbl_contentinfo: {target_table}")
                                break
                    except:
                        continue
            
            if not target_table:
                steps.append(f"ERREUR: tbl_contentinfo introuvable pour {app_id}")
                return {"success": True, "steps": steps, "db_modified": False}
            
            # Get current columns
            cursor = await db.execute(f"PRAGMA table_info({target_table})")
            existing_cols = [c[1] for c in await cursor.fetchall()]
            
            for target_col, new_val in target_updates:
                # CREATE column if it doesn't exist
                if target_col not in existing_cols:
                    try:
                        await db.execute(f"ALTER TABLE {target_table} ADD COLUMN {target_col} TEXT")
                        steps.append(f"CREE colonne {target_table}.{target_col}")
                        existing_cols.append(target_col)
                    except Exception as e:
                        steps.append(f"Erreur creation colonne: {e}")
                        continue
                
                # UPDATE the column with the new path
                try:
                    await db.execute(
                        f"UPDATE {target_table} SET {target_col} = ? WHERE CAST({where_col} AS TEXT) LIKE ?",
                        (new_val, f"%{app_id}%")
                    )
                    updated += 1
                    steps.append(f"UPDATE {target_table}.{target_col} = {new_val}")
                except Exception as e:
                    steps.append(f"Erreur update: {e}")
            
            await db.commit()
        
        if updated == 0:
            steps.append("Aucune modification effectuee")
            return {"success": True, "steps": steps, "db_modified": False}
        
        # Re-upload modified app.db to PS5
        steps.append("Upload app.db vers PS5...")
        ftp = await fresh_ftp(session_id)
        if not ftp:
            steps.append("ERREUR: FTP reconnexion echouee")
            return {"success": True, "steps": steps, "db_modified": True, "db_uploaded": False}
        
        with open(db_path, 'rb') as f:
            db_data = f.read()
        
        await ftp_upload(ftp, db_data, "/system_data/priv/mms/app.db", retries=3, delay=1.0)
        steps.append(f"app.db uploadee ({len(db_data)} bytes)")
        steps.append("Redemarrez la PS5 pour appliquer")
        
        return {"success": True, "steps": steps, "db_modified": True, "db_uploaded": True}
    
    except Exception as e:
        logger.error(f"Force DB path error: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.get("/ftp/ping")
async def ftp_ping(session_id: str):
    """Check if PS5 FTP connection is still alive - WITHOUT disrupting existing connection"""
    if session_id not in ftp_connections:
        return {"alive": False}
    try:
        # Just check if session exists and try a lightweight operation
        # Do NOT use fresh_ftp() here - it kills active connections!
        client = ftp_connections[session_id]["client"]
        await asyncio.wait_for(client.list("/"), timeout=5.0)
        return {"alive": True}
    except:
        # Connection dead - try to reconnect silently
        try:
            conn = ftp_connections[session_id]
            new_client = aioftp.Client()
            await new_client.connect(conn["ip"], conn["port"])
            ftp_connections[session_id]["client"] = new_client
            return {"alive": True}
        except:
            try:
                del ftp_connections[session_id]
            except:
                pass
            return {"alive": False}

@api_router.get("/ftp/list-backups/{app_id}")
async def list_backups(session_id: str, app_id: str):
    """List all backup files (.bak_) for an app"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        backups = []
        # Scan all possible directories for .bak_ files
        db_scan = await query_db_for_app(app_id)
        concept_ids = db_scan.get("concept_ids", [])
        
        dirs = [
            f"/user/appmeta/{app_id}",
            f"/user/app/{app_id}/sce_sys",
        ]
        for cid in concept_ids:
            dirs.append(f"/user/catalog_downloader/conceptmeta/{cid}")
        
        for dir_path in dirs:
            try:
                files = []
                async for path, info in ftp.list(dir_path):
                    fname = str(path).split('/')[-1]
                    if '.bak_' in fname:
                        # Extract original filename and timestamp
                        parts = fname.split('.bak_')
                        original = parts[0]
                        timestamp = parts[1] if len(parts) > 1 else '?'
                        size = info.get('size', 0)
                        backups.append({
                            "file": fname,
                            "original": original,
                            "timestamp": timestamp,
                            "path": f"{dir_path}/{fname}",
                            "restore_to": f"{dir_path}/{original}",
                            "size": int(size) if size else 0,
                            "dir": dir_path,
                        })
            except:
                try:
                    ftp = await fresh_ftp(session_id)
                except:
                    pass
        
        return {"success": True, "backups": backups}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/restore-backup/{app_id}")
async def restore_backup(session_id: str, app_id: str, backup_path: str = Form(...), restore_to: str = Form(...)):
    """Restore a backup file to its original location"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        steps = []
        
        # Download backup
        data = await ftp_download(ftp, backup_path, retries=3, delay=0.5)
        if not data:
            return JSONResponse(status_code=404, content={"success": False, "detail": f"Backup introuvable: {backup_path}"})
        steps.append(f"Telecharge backup ({len(data)}b)")
        
        # Upload to original path
        await asyncio.sleep(0.3)
        ftp = await fresh_ftp(session_id)
        await ftp_upload(ftp, data, restore_to, retries=3, delay=1.0)
        steps.append(f"Restaure: {restore_to}")
        
        try:
            await ftp.change_permissions(restore_to, 0o444)
        except:
            pass
        
        # Delete backup file
        try:
            await asyncio.sleep(0.3)
            ftp = await fresh_ftp(session_id)
            await ftp.remove(backup_path)
            steps.append(f"Backup supprime: {backup_path.split('/')[-1]}")
        except:
            steps.append("Backup conserve (suppression echouee)")
        
        return {"success": True, "steps": steps}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/restore-all/{app_id}")
async def restore_all_backups(session_id: str, app_id: str):
    """Restore ALL backups for an app (most recent backup per original file)"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    try:
        # First list all backups
        list_resp = await list_backups(session_id, app_id)
        if isinstance(list_resp, JSONResponse):
            return list_resp
        
        backups = list_resp.get("backups", [])
        if not backups:
            return {"success": True, "steps": ["Aucun backup trouve"], "restored": 0}
        
        # Group by original file, keep most recent
        by_original = {}
        for bk in backups:
            key = bk["restore_to"]
            if key not in by_original or bk["timestamp"] > by_original[key]["timestamp"]:
                by_original[key] = bk
        
        steps = []
        restored = 0
        
        for restore_to, bk in by_original.items():
            try:
                ftp = await fresh_ftp(session_id)
                if not ftp:
                    steps.append("FTP perdu")
                    break
                
                data = await ftp_download(ftp, bk["path"], retries=2, delay=0.5)
                if data:
                    await asyncio.sleep(0.3)
                    ftp = await fresh_ftp(session_id)
                    await ftp_upload(ftp, data, restore_to, retries=2, delay=0.5)
                    try:
                        await ftp.change_permissions(restore_to, 0o444)
                    except:
                        pass
                    restored += 1
                    steps.append(f"Restaure: {bk['original']} ({bk['dir'].split('/')[-1]})")
                    
                    # Delete backup
                    try:
                        await asyncio.sleep(0.3)
                        ftp = await fresh_ftp(session_id)
                        await ftp.remove(bk["path"])
                    except:
                        pass
            except Exception as e:
                steps.append(f"Erreur {bk['original']}: {e}")
            await asyncio.sleep(0.3)
        
        steps.append(f"Total: {restored} fichier(s) restaure(s)")
        return {"success": True, "steps": steps, "restored": restored}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

# ===== PARAM.SFO PARSER =====
def parse_sfo(data: bytes) -> dict:
    """Parse a PS4/PS5 param.sfo binary file and return key-value pairs"""
    if len(data) < 0x14 or data[0:4] != b'\x00PSF':
        return {}
    
    version = struct.unpack_from('<I', data, 4)[0]
    key_table_start = struct.unpack_from('<I', data, 8)[0]
    data_table_start = struct.unpack_from('<I', data, 12)[0]
    tables_entries = struct.unpack_from('<I', data, 16)[0]
    
    entries = {}
    for i in range(tables_entries):
        offset = 0x14 + i * 0x10
        key_offset = struct.unpack_from('<H', data, offset)[0]
        data_fmt = struct.unpack_from('<H', data, offset + 2)[0]
        data_len = struct.unpack_from('<I', data, offset + 4)[0]
        data_max_len = struct.unpack_from('<I', data, offset + 8)[0]
        data_offset = struct.unpack_from('<I', data, offset + 12)[0]
        
        # Read key name
        key_end = data.index(b'\x00', key_table_start + key_offset)
        key_name = data[key_table_start + key_offset:key_end].decode('utf-8', errors='replace')
        
        # Read value
        val_start = data_table_start + data_offset
        if data_fmt == 0x0204:  # UTF-8 string
            val_end = val_start + data_len
            val = data[val_start:val_end].rstrip(b'\x00').decode('utf-8', errors='replace')
        elif data_fmt == 0x0404:  # Integer
            val = struct.unpack_from('<I', data, val_start)[0]
        else:
            val = data[val_start:val_start + data_len].hex()
        
        entries[key_name] = {"value": val, "fmt": data_fmt, "max_len": data_max_len, "data_offset": data_offset}
    
    return entries

def modify_sfo_title(data: bytearray, new_title: str) -> bytearray:
    """Modify the TITLE field in a param.sfo binary"""
    if len(data) < 0x14 or data[0:4] != b'\x00PSF':
        return data
    
    key_table_start = struct.unpack_from('<I', data, 8)[0]
    data_table_start = struct.unpack_from('<I', data, 12)[0]
    tables_entries = struct.unpack_from('<I', data, 16)[0]
    
    for i in range(tables_entries):
        offset = 0x14 + i * 0x10
        key_offset = struct.unpack_from('<H', data, offset)[0]
        data_fmt = struct.unpack_from('<H', data, offset + 2)[0]
        data_max_len = struct.unpack_from('<I', data, offset + 8)[0]
        data_offset = struct.unpack_from('<I', data, offset + 12)[0]
        
        key_end = data.index(b'\x00', key_table_start + key_offset)
        key_name = data[key_table_start + key_offset:key_end].decode('utf-8', errors='replace')
        
        if key_name == "TITLE" and data_fmt == 0x0204:
            val_start = data_table_start + data_offset
            new_bytes = new_title.encode('utf-8')
            if len(new_bytes) >= data_max_len:
                new_bytes = new_bytes[:data_max_len - 1]
            # Write new title + null padding
            padded = new_bytes + b'\x00' * (data_max_len - len(new_bytes))
            data[val_start:val_start + data_max_len] = padded
            # Update data_len in index
            struct.pack_into('<I', data, offset + 4, len(new_bytes) + 1)
            break
    
    return data

@api_router.get("/ftp/scan-sfo/{app_id}")
async def scan_sfo(session_id: str, app_id: str):
    """Scan PS5 for param.sfo, download and parse it"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        # Possible locations for param.sfo
        sfo_paths = [
            f"/user/app/{app_id}/sce_sys/param.sfo",
            f"/system_data/priv/appmeta/{app_id}/param.sfo",
            f"/user/appmeta/{app_id}/param.sfo",
        ]
        
        found_path = None
        sfo_data = None
        checked = []
        
        for path in sfo_paths:
            try:
                data = await ftp_download(ftp, path, retries=1, delay=0.3)
                if data and len(data) > 0x14:
                    found_path = path
                    sfo_data = data
                    checked.append(f"OK: {path} ({len(data)}b)")
                    break
                else:
                    checked.append(f"Vide: {path}")
            except:
                checked.append(f"404: {path}")
                try:
                    ftp = await fresh_ftp(session_id)
                except:
                    pass
        
        if not sfo_data:
            return {"success": True, "found": False, "checked": checked}
        
        # Parse SFO
        entries = parse_sfo(sfo_data)
        title = entries.get("TITLE", {}).get("value", "???")
        title_max = entries.get("TITLE", {}).get("max_len", 0)
        
        # Get title from DB too
        db_title = None
        db_path = get_cached_db()
        if db_path:
            try:
                async with aiosqlite.connect(db_path) as db:
                    cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [r[0] for r in await cursor.fetchall()]
                    for table in tables:
                        if "contentinfo" in table.lower() and "concept" not in table.lower():
                            try:
                                cursor = await db.execute(f"PRAGMA table_info({table})")
                                cols = [c[1] for c in await cursor.fetchall()]
                                if "AppInfoJson" in cols:
                                    cursor = await db.execute(
                                        f"SELECT AppInfoJson FROM {table} WHERE CAST(metaDataPath AS TEXT) LIKE ? OR CAST(icon0Info AS TEXT) LIKE ?",
                                        (f"%{app_id}%", f"%{app_id}%")
                                    )
                                    row = await cursor.fetchone()
                                    if row and row[0]:
                                        info = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                                        if isinstance(info, list):
                                            for item in info:
                                                if isinstance(item, dict) and item.get("key") == "TITLE":
                                                    db_title = item.get("data", "")
                                                    break
                                        elif isinstance(info, dict):
                                            db_title = info.get("TITLE", "")
                            except:
                                pass
            except:
                pass
        
        # Build SFO summary
        sfo_info = {}
        for k, v in entries.items():
            sfo_info[k] = v["value"]
        
        return {
            "success": True,
            "found": True,
            "path": found_path,
            "title": title,
            "title_max_len": title_max,
            "db_title": db_title,
            "sfo_fields": sfo_info,
            "checked": checked,
        }
    except Exception as e:
        logger.error(f"SFO scan error: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

app.include_router(api_router)

@app.get("/")
async def index():
    return FileResponse(ROOT_DIR / "index.html", headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True)
