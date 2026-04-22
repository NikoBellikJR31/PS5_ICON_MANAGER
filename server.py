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
import ftplib

import sys as _sys
if getattr(_sys, 'frozen', False):
    ROOT_DIR = Path(_sys.executable).parent
else:
    ROOT_DIR = Path(__file__).parent
CACHE_DIR = ROOT_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

app = FastAPI()
api_router = APIRouter(prefix="/api")
ftp_connections = {}
user_home_cache = {}  

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

async def ftp_upload_raw(session_id, data, path):
    """Upload via ftplib avec CWD + STOR - ferme aioftp d'abord"""
    ip, port = None, None
    if session_id in ftp_connections:
        conn = ftp_connections[session_id]
        ip, port = conn["ip"], conn["port"]
        
        try:
            await conn["client"].quit()
        except:
            pass
    if not ip:
        return False
    
    def _do_upload():
        try:
            f = ftplib.FTP()
            f.connect(ip, port, timeout=10)
            f.login()
        
            parts = path.rsplit('/', 1)
            if len(parts) == 2:
                f.cwd(parts[0])
                f.storbinary(f"STOR {parts[1]}", io.BytesIO(data))
            else:
                f.storbinary(f"STOR {path}", io.BytesIO(data))
            f.quit()
            logger.info(f"ftplib upload OK: {path}")
            return True
        except Exception as e:
            logger.warning(f"ftplib upload echec {path}: {e}")
            return False
    
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _do_upload)
    
    if session_id in ftp_connections:
        try:
            new_ftp = aioftp.Client()
            await new_ftp.connect(ip, port)
            ftp_connections[session_id]["client"] = new_ftp
        except:
            pass
    
    return result

async def ftp_chmod(session_id_or_ftp, path, mode="0444"):
    """SITE CHMOD via ftplib + reconnexion aioftp apres"""
    await asyncio.sleep(0.3)
    
    ip, port = None, None
    sid = None
    if isinstance(session_id_or_ftp, str) and session_id_or_ftp in ftp_connections:
        sid = session_id_or_ftp
        conn = ftp_connections[sid]
        ip, port = conn["ip"], conn["port"]
    else:
        for s, conn in ftp_connections.items():
            sid = s
            ip, port = conn["ip"], conn["port"]
            break
    
    if not ip:
        return False
    
    def _do_chmod():
        try:
            f = ftplib.FTP()
            f.connect(ip, port, timeout=5)
            f.login()
            parts = path.rsplit('/', 1)
            if len(parts) == 2:
                f.cwd(parts[0])
                resp = f.sendcmd(f"SITE CHMOD {mode} {parts[1]}")
            else:
                resp = f.sendcmd(f"SITE CHMOD {mode} {path}")
            logger.info(f"CHMOD {mode} -> {path}: {resp}")
            f.quit()
            return True
        except Exception as e:
            logger.warning(f"CHMOD {mode} echec {path}: {e}")
            return False
    
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _do_chmod)
    
    if sid and sid in ftp_connections:
        try:
            new_ftp = aioftp.Client()
            await new_ftp.connect(ip, port)
            ftp_connections[sid]["client"] = new_ftp
        except:
            pass
    
    return result

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


@api_router.get("/launch-theme-gen")
async def launch_theme_gen():
    """Lance theme_generator.py"""
    import subprocess as sp
    try:
        import sys as _sys2
        base = Path(_sys2.executable).parent if getattr(_sys2, 'frozen', False) else Path(__file__).parent
        script = base / "theme_generator.py"
        if script.exists():
            sp.Popen([_sys2.executable, str(script)], cwd=str(script.parent))
            return {"success": True}
        return {"success": False, "detail": "theme_generator.py non trouve"}
    except Exception as e:
        return {"success": False, "detail": str(e)}

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
                                        if v and isinstance(v, str) and v.startswith('/') and len(v) < 200 and 'field_list' not in v:
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
      
                bak_file = CACHE_DIR / "app.db.bak"
                shutil.copy2(db_file, bak_file)
                logger.info("app.db.bak cree (backup frais)")
                db_status += " + bak"
        except Exception as e:
            db_status = f"erreur: {e}"
            logger.warning(f"Could not cache app.db: {e}")
        
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
        
        db_scan = await query_db_for_app(app_id)
        concept_ids = db_scan["concept_ids"]
        logger.info(f"ConceptIds from cache: {concept_ids}")
        
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
        
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP reconnect failed"})
        
        all_found = []
        for dir_path in dirs:
            files = await ftp_list_dir(ftp, dir_path)
            
            img_files = [f for f in files if f["name"].lower().endswith(('.png', '.dds', '.jpg', '.jpeg', '.webp', '.bmp')) and 
                        any(f["name"].lower().startswith(t) for t in ["icon0", "pic0", "pic1", "pic2", "save_data"])]
            
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
                real_w, real_h = 0, 0
                need_preview = img_type and img_type not in preview_done
                
                if need_preview:
                    try:
                        img_data = await ftp_download(ftp, full_path, retries=3, delay=0.5)
                        if img_data:
                            size = len(img_data)
                            if fname.endswith('.dds'):
                                b64 = dds_to_png_base64(img_data)
                                if len(img_data) >= 20:
                                    real_h = struct.unpack_from('<I', img_data, 12)[0]
                                    real_w = struct.unpack_from('<I', img_data, 16)[0]
                            else:
                                b64 = base64.b64encode(img_data).decode('utf-8')
                                try:
                                    pimg = Image.open(io.BytesIO(img_data))
                                    real_w, real_h = pimg.size
                                except:
                                    pass
                            preview_done[img_type] = True
                    except:
                        try:
                            ftp = await fresh_ftp(session_id)
                        except:
                            pass
                
                all_found.append({
                    "dir": dir_path, "file": f["name"],
                    "full_path": full_path, "size": size, "data": b64,
                    "real_w": real_w, "real_h": real_h,
                })
        
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
        paths = [p.strip() for p in target_paths.split(',') if p.strip()]
        
        has_png = any(p.lower().endswith('.png') for p in paths)
        has_dds = any(p.lower().endswith('.dds') for p in paths)
        has_jpg = any(p.lower().endswith(('.jpg', '.jpeg')) for p in paths)
        
        w, h = SIZES.get(image_type, (512, 512))
        
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        orig_data_cache = {}  
        for target_path in paths:
            try:
                old_data = await ftp_download(ftp, target_path, retries=1, delay=0.2)
                if old_data and len(old_data) > 128:
                    orig_data_cache[target_path] = old_data
                    # Extraire dimensions
                    if target_path.lower().endswith('.dds') and len(old_data) >= 20:
                        orig_h = struct.unpack_from('<I', old_data, 12)[0]
                        orig_w = struct.unpack_from('<I', old_data, 16)[0]
                        if 64 <= orig_w <= 4096 and 64 <= orig_h <= 4096:
                            w, h = orig_w, orig_h
                            steps.append(f"Dimensions originales (DDS): {w}x{h}")
                    else:
                        try:
                            orig_img = Image.open(io.BytesIO(old_data))
                            ow, oh = orig_img.size
                            if 64 <= ow <= 4096 and 64 <= oh <= 4096:
                                w, h = ow, oh
                                steps.append(f"Dimensions originales: {w}x{h}")
                        except:
                            pass
                    break
            except:
                try:
                    ftp = await fresh_ftp(session_id)
                except:
                    pass
        
        data = await file.read()
        img = Image.open(io.BytesIO(data))
        if img.mode != 'RGBA':
            img = img.convert('RGBA')
        img = img.resize((w, h), Image.Resampling.LANCZOS)
        steps.append(f"Redimensionne {w}x{h}")
        
        png_bytes = None
        dds_bytes = None
        if has_png or has_jpg or (not has_dds):
            png_buf = io.BytesIO()
            img.save(png_buf, format="PNG")
            png_bytes = png_buf.getvalue()
        if has_dds:
            dds_bytes = png_to_dds_bytes(img)
        
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP reconnect failed"})
        
        replaced = 0
        for target_path in paths:
            try:
                is_dds = target_path.lower().endswith('.dds')
                file_bytes = dds_bytes if is_dds else png_bytes
                if file_bytes is None:
                    steps.append(f"Skip {target_path.split('/')[-1]} (format non genere)")
                    continue
                
                await ftp_chmod(session_id, target_path, "0777")
                ftp = await fresh_ftp(session_id)
                
                try:
                    old = orig_data_cache.get(target_path)
                    if not old:
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
                
                
            except Exception as e:
                steps.append(f"Erreur {target_path.split('/')[-1]}: {e}")
                try:
                    ftp = await fresh_ftp(session_id)
                except:
                    break
        
        sce_sys_path = f"/user/app/{app_id}/sce_sys"
        try:
            await ftp.make_directory(sce_sys_path)
        except:
            pass
        sce_formats = []
        if has_png and png_bytes:
            sce_formats.append((".png", png_bytes))
        if has_dds and dds_bytes:
            sce_formats.append((".dds", dds_bytes))
        if not sce_formats and png_bytes:
            sce_formats.append((".png", png_bytes))
        
        for ext, fb in sce_formats:
            sce_target = f"{sce_sys_path}/{image_type}{ext}"
            try:
                await ftp_upload(ftp, fb, sce_target)
                steps.append(f"+ sce_sys: {image_type}{ext}")
                replaced += 1
            except:
                try:
                    ftp = await fresh_ftp(session_id)
                except:
                    pass
        
        steps.append(f"Total: {replaced} fichiers")
        
        all_chmod_paths = list(paths) + [f"{sce_sys_path}/{image_type}{ext}" for ext, fb in sce_formats]
        try:
            await ftp.quit()
        except:
            pass
        for cp in all_chmod_paths:
            await ftp_chmod(session_id, cp, "0444")
        steps.append("CHMOD 444 applique")
        
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
        
        all_targets = set()
        
        if paths:
            for p in paths.split(','):
                p = p.strip()
                if p:
                    all_targets.add(p)
        
        locations = [f"/user/appmeta/{app_id}", f"/user/app/{app_id}/sce_sys"]
        for loc in locations:
            for ext in [".png", ".dds", ".jpg", ".jpeg"]:
                all_targets.add(f"{loc}/{image_type}{ext}")
        
        for target in all_targets:
            try:
                await ftp.remove(target)
                deleted += 1
                steps.append(f"Supprime: {target.split('/')[-1]}")
            except:
                pass
        
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
    
    new_dds_path = f"/user/appmeta/{app_id}/{image_type}.dds?ts={ts}"
    new_png_path = f"/user/appmeta/{app_id}/{image_type}.png?ts={ts}"
    
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
            
            target_table = None
            where_col = None
            
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
         
            cursor = await db.execute(f"PRAGMA table_info({target_table})")
            existing_cols = [c[1] for c in await cursor.fetchall()]
            
            for target_col, new_val in target_updates:
                
                if target_col not in existing_cols:
                    try:
                        await db.execute(f"ALTER TABLE {target_table} ADD COLUMN {target_col} TEXT")
                        steps.append(f"CREE colonne {target_table}.{target_col}")
                        existing_cols.append(target_col)
                    except Exception as e:
                        steps.append(f"Erreur creation colonne: {e}")
                        continue
                
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
        client = ftp_connections[session_id]["client"]
        await asyncio.wait_for(client.list("/"), timeout=5.0)
        return {"alive": True}
    except:
        
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
        
        groups = {}
        for bk in backups:
            groups.setdefault(bk["restore_to"], []).append(bk)
        kept = []
        to_delete = []
        for key, grp in groups.items():
            grp.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            kept.extend(grp[:5])
            to_delete.extend(grp[5:])
        for bk in to_delete:
            try:
                await asyncio.sleep(0.1)
                ftp = await fresh_ftp(session_id)
                if ftp:
                    await ftp.remove(bk["path"])
            except:
                pass
        
        return {"success": True, "backups": kept}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.get("/ftp/backup-preview")
async def backup_preview(session_id: str, path: str):
    """Download a backup file and return base64 preview"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return {"success": False, "detail": "FTP failed"}
        data = await ftp_download(ftp, path, retries=2, delay=0.4)
        if not data:
            return {"success": False, "detail": "download failed"}
        low = path.lower()
        b64 = None
        if '.dds' in low:
            b64 = dds_to_png_base64(data)
        else:
            try:
                pimg = Image.open(io.BytesIO(data))
                pimg.thumbnail((512, 512))
                out = io.BytesIO()
                pimg.save(out, format='PNG')
                out.seek(0)
                b64 = base64.b64encode(out.read()).decode('utf-8')
            except:
                b64 = None
        if not b64:
            return {"success": False, "detail": "not an image"}
        return {"success": True, "data": "data:image/png;base64," + b64}
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
        
        data = await ftp_download(ftp, backup_path, retries=3, delay=0.5)
        if not data:
            return JSONResponse(status_code=404, content={"success": False, "detail": f"Backup introuvable: {backup_path}"})
        steps.append(f"Telecharge backup ({len(data)}b)")
        
        await ftp_chmod(session_id, restore_to, "0777")
        ftp = await fresh_ftp(session_id)
        await ftp_upload(ftp, data, restore_to, retries=3, delay=1.0)
        steps.append(f"Restaure: {restore_to}")
        
        await ftp_chmod(session_id, restore_to, "0444")
        
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
        
        list_resp = await list_backups(session_id, app_id)
        if isinstance(list_resp, JSONResponse):
            return list_resp
        
        backups = list_resp.get("backups", [])
        if not backups:
            return {"success": True, "steps": ["Aucun backup trouve"], "restored": 0}
        
        
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
                    
                    await ftp_chmod(session_id, restore_to, "0777")
                    ftp = await fresh_ftp(session_id)
                    await ftp_upload(ftp, data, restore_to, retries=2, delay=0.5)
                    
                    await ftp_chmod(session_id, restore_to, "0444")
                    restored += 1
                    steps.append(f"Restaure: {bk['original']} ({bk['dir'].split('/')[-1]})")
                    
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
        
        key_end = data.index(b'\x00', key_table_start + key_offset)
        key_name = data[key_table_start + key_offset:key_end].decode('utf-8', errors='replace')
        
        val_start = data_table_start + data_offset
        if data_fmt == 0x0204:  
            val_end = val_start + data_len
            val = data[val_start:val_end].rstrip(b'\x00').decode('utf-8', errors='replace')
        elif data_fmt == 0x0404:  
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
        
        entries = parse_sfo(sfo_data)
        title = entries.get("TITLE", {}).get("value", "???")
        title_max = entries.get("TITLE", {}).get("max_len", 0)
        
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

@api_router.post("/ftp/restore-db")
async def restore_db_backup(session_id: str):
    """Restaurer app.db depuis app.db.bak"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    bak_path = CACHE_DIR / "app.db.bak"
    db_path = CACHE_DIR / "app.db"
    
    if not bak_path.exists():
        return JSONResponse(status_code=404, content={"success": False, "detail": "Aucun backup app.db.bak trouve"})
    
    steps = []
    try:
        
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        pre_restore = CACHE_DIR / f"app.db.pre_restore_{ts}"
        shutil.copy2(str(db_path), str(pre_restore))
        steps.append(f"Sauvegarde pre-restore: {pre_restore.name}")
       
        shutil.copy2(str(bak_path), str(db_path))
        steps.append("app.db restauree depuis app.db.bak")
        
        ftp = await fresh_ftp(session_id)
        if not ftp:
            steps.append("ERREUR: FTP reconnexion echouee - DB restauree localement seulement")
            return {"success": True, "steps": steps, "uploaded": False}
        
        with open(str(db_path), 'rb') as f:
            db_data = f.read()
        await ftp_upload(ftp, db_data, "/system_data/priv/mms/app.db", retries=3, delay=1.0)
        steps.append(f"app.db uploadee vers PS5 ({len(db_data)} bytes)")
        steps.append("Redemarrez la PS5 pour appliquer")
        
        return {"success": True, "steps": steps, "uploaded": True}
    except Exception as e:
        logger.error(f"Restore DB error: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/rename-app/{app_id}")
async def rename_app(session_id: str, app_id: str, new_name: str = Form(...)):
    """Renommer une app dans la DB uniquement (jeux seulement, champs existants)"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    db_path = get_cached_db()
    if not db_path:
        return JSONResponse(status_code=400, content={"success": False, "detail": "DB pas en cache"})
    
    steps = []
    try:
        
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = str(CACHE_DIR / f"app.db.bak_{ts}")
        shutil.copy2(db_path, backup)
        steps.append(f"Backup: {backup.split('/')[-1]}")
        
        updated = 0
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in await cursor.fetchall()]
            
            for table in tables:
                
                tl = table.lower()
                if not ("contentinfo" in tl or "conceptmetadata" in tl or "iconinfo" in tl):
                    continue
                try:
                    cursor = await db.execute(f"PRAGMA table_info({table})")
                    cols = [c[1] for c in await cursor.fetchall()]
                    
                    where_col = None
                    for wc in ["icon0Info", "metaDataPath", "titleId", "localConceptId", "conceptId"]:
                        if wc in cols:
                            cursor = await db.execute(
                                f"SELECT COUNT(*) FROM {table} WHERE CAST({wc} AS TEXT) LIKE ?",
                                (f"%{app_id}%",)
                            )
                            cnt = (await cursor.fetchone())[0]
                            if cnt > 0:
                                where_col = wc
                                break
                    if not where_col:
                        continue
                    
                    name_cols = [c for c in cols if c.lower() in ("titlename", "title", "name", "conceptname")]
                    for nc in name_cols:
                        await db.execute(
                            f"UPDATE {table} SET {nc} = ? WHERE CAST({where_col} AS TEXT) LIKE ?",
                            (new_name, f"%{app_id}%")
                        )
                        updated += 1
                        steps.append(f"UPDATE {table}.{nc} = '{new_name}'")
                    
                    if "AppInfoJson" in cols:
                        cursor = await db.execute(
                            f"SELECT rowid, AppInfoJson FROM {table} WHERE CAST({where_col} AS TEXT) LIKE ?",
                            (f"%{app_id}%",)
                        )
                        rows = await cursor.fetchall()
                        for row in rows:
                            rowid, raw_json = row[0], row[1]
                            if not raw_json:
                                continue
                            try:
                                info = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
                                modified = False
                                
                                name_keys = ("TITLE", "TITLE_00", "concept_name", "_concept_name")
                                if isinstance(info, list):
                                    for item in info:
                                        if isinstance(item, dict) and item.get("key") in name_keys:
                                            item["data"] = new_name
                                            modified = True
                                            steps.append(f"AppInfoJson.{item['key']} = '{new_name}'")
                                elif isinstance(info, dict):
                                    for nk in name_keys:
                                        if nk in info:
                                            info[nk] = new_name
                                            modified = True
                                            steps.append(f"AppInfoJson.{nk} = '{new_name}'")
                                
                                if modified:
                                    new_json = json.dumps(info, ensure_ascii=False)
                                    await db.execute(
                                        f"UPDATE {table} SET AppInfoJson = ? WHERE rowid = ?",
                                        (new_json, rowid)
                                    )
                                    updated += 1
                            except Exception as ej:
                                steps.append(f"AppInfoJson parse error: {ej}")
                except:
                    continue
            
            await db.commit()
        
        if updated == 0:
            steps.append("Aucun champ de nom trouve dans la DB pour ce jeu")
            return {"success": True, "steps": steps, "renamed": False}
        
        ftp = await fresh_ftp(session_id)
        if ftp:
            with open(db_path, 'rb') as f:
                db_data = f.read()
            await ftp_upload(ftp, db_data, "/system_data/priv/mms/app.db", retries=3, delay=1.0)
            steps.append(f"app.db uploadee ({len(db_data)} bytes)")
            steps.append("Redemarrez la PS5 pour voir le nouveau nom")
        else:
            steps.append("FTP perdu - DB modifiee localement")
        
        return {"success": True, "steps": steps, "renamed": True}
    except Exception as e:
        logger.error(f"Rename error: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/toggle-visibility/{app_id}")
async def toggle_visibility(session_id: str, app_id: str, visible: str = Form(...)):
    """Masquer/afficher une app dans la DB (visible=0 ou visible=1)"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    db_path = get_cached_db()
    if not db_path:
        return JSONResponse(status_code=400, content={"success": False, "detail": "DB pas en cache"})
    
    vis_val = int(visible) if visible in ("0", "1") else 1
    steps = []
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = str(CACHE_DIR / f"app.db.bak_{ts}")
        shutil.copy2(db_path, backup)
        steps.append(f"Backup: {backup.split('/')[-1]}")
        
        updated = 0
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in await cursor.fetchall()]
            
            for table in tables:
                tl = table.lower()
                if "iconinfo" not in tl:
                    continue
                try:
                    cursor = await db.execute(f"PRAGMA table_info({table})")
                    cols = [c[1] for c in await cursor.fetchall()]
                    
                    vis_col = None
                    for vc in ["visible", "isVisible", "hidden", "disabled"]:
                        if vc in cols:
                            vis_col = vc
                            break
                    
                    where_col = None
                    for wc in ["titleId", "icon0Info", "metaDataPath", "localConceptId", "conceptId"]:
                        if wc in cols:
                            cursor = await db.execute(
                                f"SELECT COUNT(*) FROM {table} WHERE CAST({wc} AS TEXT) LIKE ?",
                                (f"%{app_id}%",)
                            )
                            cnt = (await cursor.fetchone())[0]
                            if cnt > 0:
                                where_col = wc
                                break
                    
                    if where_col and vis_col:
                        val = vis_val if vis_col != "hidden" else (1 - vis_val)
                        await db.execute(
                            f"UPDATE {table} SET {vis_col} = ? WHERE CAST({where_col} AS TEXT) LIKE ?",
                            (val, f"%{app_id}%")
                        )
                        updated += 1
                        steps.append(f"{table}.{vis_col} = {val}")
                except:
                    continue
            
            await db.commit()
        
        if updated == 0:
            steps.append("Aucune colonne visible/hidden trouvee dans la DB")
            steps.append("La PS5 n'utilise peut-etre pas ce systeme")
            return {"success": True, "steps": steps, "toggled": False}
        
        ftp = await fresh_ftp(session_id)
        if ftp:
            with open(db_path, 'rb') as f:
                db_data = f.read()
            await ftp_upload(ftp, db_data, "/system_data/priv/mms/app.db", retries=3, delay=1.0)
            steps.append(f"DB uploadee ({len(db_data)} bytes)")
            steps.append("Redemarrez la PS5")
        
        return {"success": True, "steps": steps, "toggled": True}
    except Exception as e:
        logger.error(f"Toggle visibility error: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/force-sys-pic0/{app_id}")
async def force_sys_pic0(session_id: str, app_id: str, pic0_path: str = Form(...)):
    """Forcer un chemin pic0 dans la DB pour une app systeme"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    db_path = get_cached_db()
    if not db_path:
        return JSONResponse(status_code=400, content={"success": False, "detail": "DB pas en cache"})
    steps = []
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(db_path, str(CACHE_DIR / f"app.db.bak_{ts}"))
        steps.append(f"Backup DB")
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                "UPDATE tbl_contentinfo SET pic0Info = ? WHERE titleId = ?",
                (pic0_path, app_id)
            )
            await db.commit()
            steps.append(f"tbl_contentinfo.pic0Info = {pic0_path}")
        ftp = await fresh_ftp(session_id)
        if ftp:
            with open(db_path, 'rb') as f:
                db_data = f.read()
            await ftp_upload(ftp, db_data, "/system_data/priv/mms/app.db", retries=3, delay=1.0)
            steps.append(f"DB uploadee ({len(db_data)} bytes)")
            steps.append("Redemarrez la PS5")
        return {"success": True, "steps": steps}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.get("/db/game-name/{app_id}")
async def get_game_name(app_id: str):
    """Recuperer le nom du jeu depuis la DB"""
    db_path = get_cached_db()
    if not db_path:
        return {"name": None}
    try:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in await cursor.fetchall()]
            for table in tables:
                if "iconinfo" not in table.lower() or "concept" in table.lower():
                    continue
                try:
                    cursor = await db.execute(
                        f"SELECT titleName FROM {table} WHERE titleId = ?",
                        (app_id,)
                    )
                    row = await cursor.fetchone()
                    if row and row[0]:
                        return {"name": row[0]}
                except:
                    continue
        return {"name": None}
    except:
        return {"name": None}

SYSTEM_APP_ENTRIES = [
    {"path": "/system_ex/rnps/apps/NPXS40016/appdb/NPXS40056", "label": "NPXS40016", "filter": None},
    {"path": "/system_ex/rnps/apps/NPXS40071/appdb/default", "label": "NPXS40071", "filter": None},
    {"path": "/system_ex/rnps/apps/NPXS40071/appdb/NPXS40139", "label": "NPXS40139", "filter": None},
    {"path": "/system_ex/rnps/apps/NPXS40075/appdb/default", "label": "NPXS40075", "filter": None},
    {"path": "/system_ex/rnps/apps/NPXS40075/assets/src/assets/texture", "label": "NPXS40075 texture", "filter": "icon0"},
    {"path": "/system_ex/rnps/apps/NPXS40047/appdb/default", "label": "NPXS40047", "filter": None},
    {"path": "/system_ex/app/NPXS40140/sce_sys", "label": "NPXS40140 Blu-ray", "filter": None},
    {"path": "/system_ex/vsh_asset", "label": "VSH Assets (BG)", "filter": "bg_"},
    {"path": "/system_ex/rnps/apps/NPXS40016/appdb/NPXS40054", "label": "NPXS40054", "filter": None},
    {"path": "/system_ex/rnps/apps/NPXS40016/appdb/NPXS40053", "label": "NPXS40053", "filter": None},
    {"path": "/system_ex/rnps/apps/NPXS40037/appdb/default", "label": "NPXS40037", "filter": None},
]

@api_router.get("/ftp/scan-system-apps")
async def scan_system_apps(session_id: str):
    """Scanner les apps systeme PS5 (chemins corriges)"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        results = []
        for entry in SYSTEM_APP_ENTRIES:
            sys_path = entry["path"]
            filt = entry.get("filter")
            result = {"app_id": entry["label"], "path": sys_path, "files": [], "error": None}
            try:
                files = await ftp_list_dir(ftp, sys_path)
                
                img_files = [f for f in files if f["name"].lower().endswith(('.png', '.dds', '.jpg', '.jpeg')) or '.bak_' in f["name"]]
                
                if filt:
                    img_files = [f for f in img_files if f["name"].lower().startswith(filt) or ('.bak_' in f["name"] and f["name"].lower().startswith(filt))]
                
                for f in img_files:
                    full_path = f"{sys_path}/{f['name']}"
                    b64 = None
                    size = 0
                    fname = f["name"].lower()
                    
                    if fname.endswith(('.png', '.jpg', '.jpeg', '.dds')):
                        try:
                            img_data = await ftp_download(ftp, full_path, retries=1, delay=0.3)
                            if img_data:
                                size = len(img_data)
                                if fname.endswith('.dds'):
                                    b64 = dds_to_png_base64(img_data)
                                else:
                                    b64 = base64.b64encode(img_data).decode('utf-8')
                        except:
                            try:
                                ftp = await fresh_ftp(session_id)
                            except:
                                pass
                    
                    result["files"].append({
                        "name": f["name"],
                        "full_path": full_path,
                        "size": size,
                        "data": b64,
                    })
            except Exception as e:
                result["error"] = str(e)
                try:
                    ftp = await fresh_ftp(session_id)
                except:
                    pass
            
            results.append(result)
        
        return {"success": True, "system_apps": results}
    except Exception as e:
        logger.error(f"System scan error: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.get("/ftp/scan-system-app/{index}")
async def scan_single_system_app(session_id: str, index: int):
    """Scanner UNE SEULE app systeme par index"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    if index < 0 or index >= len(SYSTEM_APP_ENTRIES):
        return JSONResponse(status_code=400, content={"success": False, "detail": "Index invalide"})
    
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        entry = SYSTEM_APP_ENTRIES[index]
        sys_path = entry["path"]
        filt = entry.get("filter")
        result = {"app_id": entry["label"], "path": sys_path, "files": [], "error": None}
        
        try:
            files = await ftp_list_dir(ftp, sys_path)
            img_files = [f for f in files if f["name"].lower().endswith(('.png', '.dds', '.jpg', '.jpeg')) or '.bak_' in f["name"]]
            if filt:
                img_files = [f for f in img_files if f["name"].lower().startswith(filt) or ('.bak_' in f["name"] and f["name"].lower().startswith(filt))]
            
            for f in img_files:
                full_path = f"{sys_path}/{f['name']}"
                b64 = None
                size = 0
                fname = f["name"].lower()
                if fname.endswith(('.png', '.jpg', '.jpeg', '.dds')) and '.bak_' not in f["name"]:
                    try:
                        img_data = await ftp_download(ftp, full_path, retries=1, delay=0.3)
                        if img_data:
                            size = len(img_data)
                            if fname.endswith('.dds'):
                                b64 = dds_to_png_base64(img_data)
                            else:
                                b64 = base64.b64encode(img_data).decode('utf-8')
                    except:
                        try:
                            ftp = await fresh_ftp(session_id)
                        except:
                            pass
                
                result["files"].append({
                    "name": f["name"],
                    "full_path": full_path,
                    "size": size,
                    "data": b64,
                })
        except Exception as e:
            result["error"] = str(e)
        
        return {"success": True, "app": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.post("/ftp/replace-system-image")
async def replace_system_image(session_id: str, target_path: str = Form(...), file: UploadFile = File(...)):
    """Remplacer une image (system ou save) avec backup local"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    try:
        steps = []
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        w, h = 512, 512
        try:
            old_data = await ftp_download(ftp, target_path, retries=2, delay=0.5)
            if old_data and len(old_data) > 128:
                
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                fname = target_path.split('/')[-1]
                bak_local = CACHE_DIR / f"{fname}.bak_{ts}"
                with open(str(bak_local), 'wb') as bf:
                    bf.write(old_data)
                steps.append(f"Backup local: {bak_local.name}")
                
                try:
                    ftp = await fresh_ftp(session_id)
                    await ftp_upload(ftp, old_data, f"{target_path}.bak_{ts}")
                    steps.append(f"Backup PS5: {fname}.bak_{ts}")
                except:
                    steps.append("Backup PS5 impossible (chemin protege)")
                
                if target_path.lower().endswith('.dds') and len(old_data) >= 20:
                    oh = struct.unpack_from('<I', old_data, 12)[0]
                    ow = struct.unpack_from('<I', old_data, 16)[0]
                    if 64 <= ow <= 4096 and 64 <= oh <= 4096:
                        w, h = ow, oh
                else:
                    try:
                        orig_img = Image.open(io.BytesIO(old_data))
                        ow, oh = orig_img.size
                        if 64 <= ow <= 4096 and 64 <= oh <= 4096:
                            w, h = ow, oh
                    except:
                        pass
        except:
            try:
                ftp = await fresh_ftp(session_id)
            except:
                pass
        
        steps.append(f"Dimensions: {w}x{h}")
        
        data = await file.read()
        img = Image.open(io.BytesIO(data))
        if img.mode != 'RGBA':
            img = img.convert('RGBA')
        img = img.resize((w, h), Image.Resampling.LANCZOS)
        
        if target_path.lower().endswith('.dds'):
            file_bytes = png_to_dds_bytes(img)
        else:
            png_buf = io.BytesIO()
            img.save(png_buf, format="PNG")
            file_bytes = png_buf.getvalue()
        
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP reconnect failed"})
        
        uploaded = False
        fallback_path = None
        
        try:
            ftp = await fresh_ftp(session_id)
            await ftp_upload(ftp, file_bytes, target_path)
            uploaded = True
            steps.append("Upload direct OK")
        except:
            pass
        
        if not uploaded:
            await ftp_chmod(session_id, target_path, "0666")
            try:
                ftp = await fresh_ftp(session_id)
                await ftp_upload(ftp, file_bytes, target_path)
                uploaded = True
                steps.append("Upload aioftp OK")
            except:
                pass
        
        if not uploaded:
            steps.append("aioftp echoue, essai ftplib...")
            ok = await ftp_upload_raw(session_id, file_bytes, target_path)
            if ok:
                uploaded = True
                steps.append("Upload ftplib OK")
        
        if not uploaded:
            path_parts = target_path.split('/')
            fallback_app_id = None
            for pp in path_parts:
                if pp.startswith('PPSA') or pp.startswith('CUSA'):
                    fallback_app_id = pp
                    break
            
            if fallback_app_id:
                fname = target_path.split('/')[-1]
                fallback_path = f"/user/app/{fallback_app_id}/sce_sys/{fname}"
                steps.append(f"Chemin source protege, fallback: {fallback_path}")
                
                await ftp_chmod(session_id, fallback_path, "0666")
                
                def _mkd():
                    try:
                        f2 = ftplib.FTP()
                        f2.connect(ip_addr, port_num, timeout=5)
                        f2.login()
                        try:
                            f2.mkd(f"/user/app/{fallback_app_id}/sce_sys")
                        except:
                            pass
                        f2.quit()
                    except:
                        pass
                
                ip_addr = ftp_connections[session_id]["ip"] if session_id in ftp_connections else None
                port_num = ftp_connections[session_id]["port"] if session_id in ftp_connections else None
                if ip_addr:
                    await asyncio.get_event_loop().run_in_executor(None, _mkd)
                
                ok = await ftp_upload_raw(session_id, file_bytes, fallback_path)
                if ok:
                    uploaded = True
                    steps.append(f"Upload fallback ftplib OK")
                else:
         
                    try:
                        ftp = await fresh_ftp(session_id)
                        await ftp_upload(ftp, file_bytes, fallback_path)
                        uploaded = True
                        steps.append(f"Upload fallback aioftp OK")
                    except:
                        pass
        
        if uploaded:
            final_path = fallback_path if fallback_path and uploaded else target_path
            steps.append(f"Remplace: {final_path.split('/')[-1]} ({len(file_bytes)}b)")
            chmod_ok = await ftp_chmod(session_id, final_path)
            if chmod_ok:
                steps.append(f"CHMOD 444 -> {final_path.split('/')[-1]}")
            if fallback_path:
                steps.append(f"NOTE: Image sauvegardee dans {fallback_path}")
                steps.append("Le chemin source etait protege en ecriture")
        else:
            steps.append(f"ECHEC: Impossible d'ecrire sur {target_path}")
            steps.append("Ce chemin est protege en ecriture")
        
        return {"success": True, "steps": steps, "uploaded": uploaded}
    except Exception as e:
        logger.error(f"Replace system image error: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@api_router.get("/ftp/scan-save-data/{app_id}")
async def scan_save_data(session_id: str, app_id: str):
    """Scan profond dans /system_ex/app/{app_id}/ pour trouver save*.png"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Not connected"})
    
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        
        base = f"/system_ex/app/{app_id}"
        found = []
        scanned = []
        
        async def scan_dir(path, depth=0):
            nonlocal ftp
            if depth > 10:
                return
            try:
                items = []
                async for item_path, info in ftp.list(path):
                    items.append({"name": item_path.name, "type": info.get("type", "file")})
                
                scanned.append(path)
                
                for item in items:
                    full = f"{path}/{item['name']}"
                    if item["type"] == "dir":
                        await scan_dir(full, depth + 1)
                    elif item["name"].lower().startswith("save") and item["name"].lower().endswith(".png"):
                        
                        b64 = None
                        size = 0
                        try:
                            img_data = await ftp_download(ftp, full, retries=2, delay=0.5)
                            if img_data:
                                size = len(img_data)
                                b64 = base64.b64encode(img_data).decode('utf-8')
                        except:
                            try:
                                ftp = await fresh_ftp(session_id)
                            except:
                                pass
                        found.append({
                            "name": item["name"],
                            "full_path": full,
                            "dir": path,
                            "size": size,
                            "data": b64,
                        })
            except:
                try:
                    ftp = await fresh_ftp(session_id)
                except:
                    pass
        
        await scan_dir(base)
        
        return {
            "success": True,
            "app_id": app_id,
            "base_path": base,
            "found": found,
            "dirs_scanned": len(scanned),
        }
    except Exception as e:
        logger.error(f"Save data scan error: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

app.include_router(api_router)

db_router = APIRouter(prefix="/api/db")

@db_router.get("/tables")
async def db_tables():
    """List all tables in cached app.db"""
    db_path = get_cached_db()
    if not db_path:
        return JSONResponse(status_code=400, content={"success": False, "detail": "DB pas en cache - reconnectez-vous"})
    try:
        tables = []
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            for row in await cursor.fetchall():
                tname = row[0]
                cursor2 = await db.execute(f"SELECT COUNT(*) FROM {tname}")
                count = (await cursor2.fetchone())[0]
                cursor3 = await db.execute(f"PRAGMA table_info({tname})")
                cols = [c[1] for c in await cursor3.fetchall()]
                tables.append({"name": tname, "rows": count, "columns": cols})
        return {"success": True, "tables": tables}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@db_router.get("/table/{table_name}")
async def db_table_data(table_name: str, search: str = "", limit: int = 100, offset: int = 0):
    """Get rows from a table with optional search"""
    db_path = get_cached_db()
    if not db_path:
        return JSONResponse(status_code=400, content={"success": False, "detail": "DB pas en cache"})
    try:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute(f"PRAGMA table_info({table_name})")
            cols = [c[1] for c in await cursor.fetchall()]
            
            if search:
                where_parts = []
                params = []
                for col in cols:
                    where_parts.append(f"CAST({col} AS TEXT) LIKE ?")
                    params.append(f"%{search}%")
                where_sql = " OR ".join(where_parts)
                
                cursor = await db.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {where_sql}", params)
                total = (await cursor.fetchone())[0]
                
                cursor = await db.execute(f"SELECT rowid, * FROM {table_name} WHERE {where_sql} LIMIT ? OFFSET ?", params + [limit, offset])
            else:
                cursor = await db.execute(f"SELECT COUNT(*) FROM {table_name}")
                total = (await cursor.fetchone())[0]
                
                cursor = await db.execute(f"SELECT rowid, * FROM {table_name} LIMIT ? OFFSET ?", [limit, offset])
            
            rows = []
            for row in await cursor.fetchall():
                r = {"_rowid": row[0]}
                for i, col in enumerate(cols):
                    val = row[i + 1]
                    if val is not None:
                        r[col] = str(val) if not isinstance(val, (int, float)) else val
                    else:
                        r[col] = None
                rows.append(r)
            
            return {"success": True, "columns": cols, "rows": rows, "total": total}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@db_router.post("/update")
async def db_update(session_id: str, table: str = Form(...), rowid: int = Form(...), column: str = Form(...), value: str = Form("")):
    """Update a single cell in app.db"""
    db_path = get_cached_db()
    if not db_path:
        return JSONResponse(status_code=400, content={"success": False, "detail": "DB pas en cache"})
    try:
       
        backup = str(CACHE_DIR / f"app.db.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        shutil.copy2(db_path, backup)
        
        val = None if value == "NULL" or value == "" else value
        async with aiosqlite.connect(db_path) as db:
            await db.execute(f"UPDATE {table} SET {column} = ? WHERE rowid = ?", (val, rowid))
            await db.commit()
        
        return {"success": True, "backup": backup.split('/')[-1]}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

@db_router.post("/upload")
async def db_upload_to_ps5(session_id: str):
    """Re-upload modified app.db to PS5"""
    if session_id not in ftp_connections:
        return JSONResponse(status_code=400, content={"success": False, "detail": "Non connecte"})
    db_path = get_cached_db()
    if not db_path:
        return JSONResponse(status_code=400, content={"success": False, "detail": "DB pas en cache"})
    try:
        ftp = await fresh_ftp(session_id)
        if not ftp:
            return JSONResponse(status_code=500, content={"success": False, "detail": "FTP failed"})
        with open(db_path, 'rb') as f:
            data = f.read()
        await ftp_upload(ftp, data, "/system_data/priv/mms/app.db", retries=3, delay=1.0)
        return {"success": True, "size": len(data)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "detail": str(e)})

app.include_router(db_router)

@app.get("/")
async def index():
    return FileResponse(ROOT_DIR / "index.html", headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=False)
