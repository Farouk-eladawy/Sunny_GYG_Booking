import asyncio
import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import re
import sqlite3
import shutil
import requests
import pyotp
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "config" / ".env")

class BookingsDatabase:
    def __init__(self, db_path: str = os.getenv("DATABASE_PATH", "bookings.db"), logger: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)
        self._init()

    def _init(self):
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS bookings (
                    id TEXT PRIMARY KEY,
                    booking_nr TEXT UNIQUE NOT NULL,
                    agency TEXT,
                    customer_name TEXT,
                    customer_country TEXT,
                    customer_phone TEXT,
                    customer_email TEXT,
                    trip_name TEXT,
                    product_id TEXT,
                    destination TEXT,
                    option_selected TEXT,
                    date_trip TEXT,
                    total_price_eur REAL,
                    retail_price REAL,
                    revenue REAL,
                    commission_breakdown REAL,
                    google_maps TEXT,
                    hotel_name TEXT,
                    guide TEXT,
                    traveler_name TEXT,
                    add_ons TEXT,
                    adt INTEGER,
                    std INTEGER,
                    chd INTEGER,
                    inf INTEGER,
                    youth INTEGER,
                    booking_status TEXT,
                    airtable_record_id TEXT,
                    synced_to_airtable INTEGER DEFAULT 0,
                    created_at TEXT,
                    updated_at TEXT,
                    raw_data TEXT
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def get_booking(self, booking_nr: str) -> Optional[Dict]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM bookings WHERE booking_nr=?", (booking_nr,))
            row = c.fetchone()
            if row:
                return dict(row)
            return None
        except Exception:
            return None
        finally:
            conn.close()

    def _compare_and_merge(self, booking: Dict, existing: Dict) -> Tuple[bool, List[str]]:
        keys_to_check = [
            "customer_name", "customer_country", "customer_phone", "customer_email",
            "trip_name", "product_id", "destination", "option_selected", "date_trip",
            "total_price_eur", "retail_price", "revenue", "commission_breakdown",
            "google_maps", "hotel_name", "guide", "traveler_name", "add_ons",
            "adt", "std", "chd", "inf", "youth", "booking_status"
        ]
        has_changes = False
        change_details = []
        import json

        for k in keys_to_check:
            v_new = booking.get(k)
            v_old = existing.get(k)
            
            # --- PROTECT EXISTING FIELDS (User Request) ---
            if k in ["adt", "chd", "inf", "youth", "std"]:
                try:
                    if v_old is not None and int(v_old) > 0 and (v_new is None or int(v_new) == 0):
                        booking[k] = v_old
                        v_new = v_old
                except (ValueError, TypeError):
                    pass

            if k in ["hotel_name", "customer_phone", "customer_name"]:
                if v_old and str(v_old).strip() and not (v_new and str(v_new).strip()):
                    booking[k] = v_old
                    v_new = v_old
                    
            def normalize_val(v):
                if v is None: return ""
                return str(v).strip()

            if isinstance(v_new, (int, float)) and isinstance(v_old, (int, float)):
                if abs(float(v_new) - float(v_old)) > 0.01:
                    has_changes = True
                    change_details.append(f"{k}: {v_old} -> {v_new}")
                    break
                continue
            elif k == "customer_email":
                s_new = normalize_val(v_new).lower()
                s_old = normalize_val(v_old).lower()
                if s_old and not s_new:
                    booking[k] = v_old
                    continue
                if s_new != s_old:
                    has_changes = True
                    change_details.append(f"{k}: {v_old} -> {v_new}")
                    break
            elif isinstance(v_new, (dict, list)):
                try:
                    obj_old = v_old
                    if isinstance(v_old, str):
                        try:
                            obj_old = json.loads(v_old)
                        except:
                            import ast
                            try:
                                obj_old = ast.literal_eval(v_old)
                            except:
                                pass
                    s_new_json = json.dumps(v_new, sort_keys=True, default=str)
                    s_old_json = json.dumps(obj_old, sort_keys=True, default=str)
                    if s_new_json != s_old_json:
                        has_changes = True
                        change_details.append(f"{k} (struct): changed")
                        break
                except Exception:
                    s_new = normalize_val(v_new)
                    s_old = normalize_val(v_old)
                    if s_new != s_old:
                        has_changes = True
                        change_details.append(f"{k}: {v_old} -> {v_new}")
                        break
            else:
                s_new = normalize_val(v_new)
                s_old = normalize_val(v_old)
                
                # Special handling for arrays/lists like add_ons that are stored as strings
                if k == "add_ons":
                    import re
                    addons_old = sorted([x.strip() for x in s_old.replace(',', ';').split(';')]) if s_old else []
                    addons_new = sorted([x.strip() for x in s_new.replace(',', ';').split(';')]) if s_new else []
                    addons_old = sorted([re.sub(r'^\d+\s*x\s*', '', x) for x in addons_old if x])
                    addons_new = sorted([re.sub(r'^\d+\s*x\s*', '', x) for x in addons_new if x])
                    if addons_old == addons_new:
                        continue
                        
                if k == "date_trip":
                    if s_old[:16] == s_new[:16]:
                        continue
                        
                if s_new != s_old:
                    try:
                        f1 = float(s_new)
                        f2 = float(s_old)
                        if abs(f1 - f2) < 0.01:
                            continue
                    except:
                        pass
                    has_changes = True
                    change_details.append(f"{k}: '{s_old}' -> '{s_new}'")
                    break

        return has_changes, change_details

    def save_booking(self, booking: Dict) -> Dict:
        existing = self.get_booking(booking.get("booking_nr"))
        status = "unchanged"
        
        if existing:
            has_changes, change_details = self._compare_and_merge(booking, existing)
            
            if has_changes:
                self.logger.info(f"Booking {booking.get('booking_nr')} changed: {'; '.join(change_details)}")
                status = "updated"
            else:
                return {
                    "success": True,
                    "status": "unchanged",
                    "synced_to_airtable": existing.get("synced_to_airtable"),
                    "airtable_record_id": existing.get("airtable_record_id"),
                    "previous_record": existing,
                }
        else:
            status = "created"

        now = datetime.now().isoformat()
        
        # Ensure complex types are serialized for SQLite
        import json
        save_params = booking.copy()
        for k, v in save_params.items():
            if isinstance(v, (dict, list)):
                save_params[k] = json.dumps(v, ensure_ascii=False)

        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute(
                """
                INSERT INTO bookings (
                    id, booking_nr, agency, customer_name, customer_country, customer_phone, customer_email,
                    trip_name, product_id, destination, option_selected, date_trip, total_price_eur,
                    retail_price, revenue, commission_breakdown, google_maps, hotel_name, guide, traveler_name,
                    add_ons, adt, std, chd, inf, youth, booking_status, airtable_record_id, synced_to_airtable,
                    created_at, updated_at, raw_data
                ) VALUES (
                    :id, :booking_nr, :agency, :customer_name, :customer_country, :customer_phone, :customer_email,
                    :trip_name, :product_id, :destination, :option_selected, :date_trip, :total_price_eur,
                    :retail_price, :revenue, :commission_breakdown, :google_maps, :hotel_name, :guide, :traveler_name,
                    :add_ons, :adt, :std, :chd, :inf, :youth, :booking_status, :airtable_record_id, :synced_to_airtable,
                    :created_at, :updated_at, :raw_data
                ) ON CONFLICT(booking_nr) DO UPDATE SET
                    agency=excluded.agency,
                    customer_name=excluded.customer_name,
                    customer_country=excluded.customer_country,
                    customer_phone=excluded.customer_phone,
                    customer_email=excluded.customer_email,
                    trip_name=excluded.trip_name,
                    product_id=excluded.product_id,
                    destination=excluded.destination,
                    option_selected=excluded.option_selected,
                    date_trip=excluded.date_trip,
                    total_price_eur=excluded.total_price_eur,
                    retail_price=excluded.retail_price,
                    revenue=excluded.revenue,
                    commission_breakdown=excluded.commission_breakdown,
                    google_maps=excluded.google_maps,
                    hotel_name=excluded.hotel_name,
                    guide=excluded.guide,
                    traveler_name=excluded.traveler_name,
                    add_ons=excluded.add_ons,
                    adt=excluded.adt,
                    std=excluded.std,
                    chd=excluded.chd,
                    inf=excluded.inf,
                    youth=excluded.youth,
                    booking_status=excluded.booking_status,
                    updated_at=excluded.updated_at,
                    raw_data=excluded.raw_data
                """,
                {
                    **save_params,
                    "airtable_record_id": booking.get("airtable_record_id"),
                    "synced_to_airtable": int(bool(booking.get("synced_to_airtable", 0))),
                    "created_at": booking.get("created_at") or now,
                    "updated_at": now,
                    "raw_data": json_safedump(booking.get("raw_data", {})),
                },
            )
            conn.commit()
            return {
                "success": True,
                "status": status,
                "previous_record": existing,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def mark_synced(self, booking_nr: str, record_id: Optional[str]):
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute(
                "UPDATE bookings SET synced_to_airtable=1, airtable_record_id=? WHERE booking_nr=?",
                (record_id, booking_nr),
            )
            conn.commit()
        finally:
            conn.close()

class AirtableManager:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.api_key = os.getenv("AIRTABLE_API_KEY")
        self.base_id = os.getenv("AIRTABLE_BASE_ID")
        self.table = os.getenv("AIRTABLE_TABLE", "Tito Sunny")
        self.api_url = f"https://api.airtable.com/v0/{self.base_id}/{quote_table(self.table)}"
        
        self.mirror_base_id = os.getenv("AIRTABLE_MIRROR_BASE_ID")
        self.mirror_api_url = f"https://api.airtable.com/v0/{self.mirror_base_id}/{quote_table(self.table)}" if self.mirror_base_id else None
        
        self.test_file = os.getenv("AIRTABLE_TEST_FILE", "airtable_sent_test.jsonl")
        self.report_file = os.getenv("AIRTABLE_SYNC_REPORT_FILE", "airtable_sync_report.jsonl")
        self._available_fields = None
        self._required_fields = [
            "Agency",
            "Booking Nr.",
            "Date Trip",
            "trip Name",
            "Add - Ons",
            "des",
            "Guide",
            "ADT",
            "STD",
            "CHD",
            "Inf",
            "Youth",
            "Customer Name",
            "Customer Country",
            "Hotel Name",
            "Product ID",
            "Total price EUR",
            "Option",
            "Customer Email",
            "Customer Phone",
            "Google Maps",
            "Booking Status",
            "Commission Breakdown",
            "Retail Price",
            "Revenue",
            "Real Product Name",
            "Real Date Trip",
        ]
        self.payload_debug_file = os.getenv("AIRTABLE_PAYLOAD_DEBUG_FILE", "airtable_payload_debug.jsonl")

    def _sync_to_mirror_base(self, booking_nr: str, fields: Dict, headers: Dict):
        """Helper to sync changes to the Mirror Base without blocking Main Base"""
        if not self.mirror_api_url:
            return
        try:
            params = {"filterByFormula": f"{{Booking Nr.}}='{booking_nr}'"}
            mirror_find = requests.get(self.mirror_api_url, headers=headers, params=params, timeout=30)
            if mirror_find.status_code == 200:
                m_data = mirror_find.json() or {}
                m_records = m_data.get("records") or []
                if m_records:
                    m_rid = m_records[0].get("id")
                    existing_m_record = m_records[0].get("fields", {})
                    
                    # Check if we actually need to patch
                    is_identical = True
                    for k, v in fields.items():
                        old_val = existing_m_record.get(k)
                        new_val = v
                        
                        if old_val is None:
                            old_val = ""
                        if new_val is None:
                            new_val = ""
                            
                        str_old = str(old_val).strip()
                        str_new = str(new_val).strip()
                        
                        if not str_new or str_new == "None":
                            continue
                            
                        # Special handling for arrays/lists (like Add - Ons, Options)
                        if isinstance(new_val, list) or isinstance(old_val, list):
                            try:
                                import json
                                list_old = old_val if isinstance(old_val, list) else (json.loads(old_val) if old_val else [])
                                list_new = new_val if isinstance(new_val, list) else (json.loads(new_val) if new_val else [])
                                # Convert to sorted strings for comparison to ignore order
                                if sorted([str(x) for x in list_old]) == sorted([str(x) for x in list_new]):
                                    continue
                            except Exception:
                                pass
                                
                        if k in ["Date Trip", "Real Date Trip"] and len(str_old) >= 16 and len(str_new) >= 16:
                            if str_old[:16] == str_new[:16]:
                                continue
                        elif k in ["Date Trip", "Real Date Trip"] and len(str_old) >= 10 and len(str_new) >= 10:
                            if str_old[:10] == str_new[:10]:
                                continue
                                
                        if isinstance(new_val, (int, float)) or (isinstance(old_val, (int, float)) and old_val != ""):
                            try:
                                f_old = float(old_val) if old_val != "" else 0.0
                                f_new = float(new_val)
                                if abs(f_old - f_new) > 0.001:
                                    is_identical = False
                                    break
                                continue
                            except ValueError:
                                pass
                                
                        if str_new != str_old:
                            is_identical = False
                            break
                            
                    if is_identical:
                        self.logger.info(f"Mirror Base sync skipped for {booking_nr} - Already up to date.")
                        return
                        
                    self.logger.info(f"Updating Mirror Base for {booking_nr}")
                    requests.patch(f"{self.mirror_api_url}/{m_rid}", headers=headers, json={"fields": fields, "typecast": True}, timeout=30)
                else:
                    self.logger.info(f"Creating record in Mirror Base for {booking_nr}")
                    requests.post(self.mirror_api_url, headers=headers, json={"fields": fields, "typecast": True}, timeout=30)
            else:
                self.logger.error(f"Failed to fetch Mirror Base for {booking_nr} during sync. Status Code: {mirror_find.status_code}. Response: {mirror_find.text}")
        except Exception as e:
            self.logger.warning(f"Failed to sync to mirror base for {booking_nr}: {e}")

    def upsert_booking(self, booking: Dict, force_update_fields: Optional[List[str]] = None) -> Dict:
        if not self.api_key or not self.base_id or not self.table:
            return {"success": False, "error": "missing_airtable_config"}
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        full_fields = {
            "Agency": booking.get("agency"),
            "Booking Nr.": booking.get("booking_nr"),
            "Customer Name": booking.get("customer_name"),
            "Customer Country": booking.get("customer_country"),
            "Customer Phone": booking.get("customer_phone"),
            "Customer Email": booking.get("customer_email"),
            "trip Name": booking.get("trip_name"),
            "Real Product Name": booking.get("trip_name"), # Backup field for original name
            "Real Date Trip": str(booking.get("date_trip")) if booking.get("date_trip") else None, # Backup for original date
            "Product ID": booking.get("product_id"),
            "des": booking.get("destination"),
            "Option": booking.get("option_selected"),
            "Date Trip": str(booking.get("date_trip")) if booking.get("date_trip") else None,
            "Total price EUR": booking.get("total_price_eur"),
            "Retail Price": str(booking.get("retail_price")) if booking.get("retail_price") is not None else None,
            "Revenue": str(booking.get("revenue")) if booking.get("revenue") is not None else None,
            "Commission Breakdown": float(booking.get("commission_breakdown")) / 100.0 if booking.get("commission_breakdown") is not None else None,
            "Google Maps": booking.get("google_maps"),
            "Hotel Name": booking.get("hotel_name"),
            "Guide": booking.get("guide"),
            "Traveler name": booking.get("traveler_name"),
            "Add - Ons": booking.get("add_ons"),
            "ADT": int(booking.get("adt")) if booking.get("adt") is not None else 0,
            "STD": int(booking.get("std")) if booking.get("std") is not None else 0,
            "CHD": int(booking.get("chd")) if booking.get("chd") is not None else 0,
            "Inf": int(booking.get("inf")) if booking.get("inf") is not None else 0,
            "Youth": int(booking.get("youth")) if booking.get("youth") is not None else 0,
            "Booking Status": booking.get("booking_status"),
        }
        try:
            if self._available_fields is None:
                params_probe = {"maxRecords": 50}
                probe = requests.get(self.api_url, headers=headers, params=params_probe, timeout=30)
                av = set()
                if probe.status_code == 200:
                    data = probe.json() or {}
                    for rec in (data.get("records") or []):
                        for k in (rec.get("fields") or {}).keys():
                            av.add(k)
                if av:
                    self._available_fields = av
                else:
                    self.logger.warning("Airtable schema detection yielded empty fields. Disabling field filtering.")
                    self._available_fields = None # Fallback to no filtering if no fields detected
        except Exception:
            self._available_fields = None
        fields = {}
        force_update_fields = force_update_fields or []
        for k in self._required_fields:
            v = full_fields.get(k)
            if v is None:
                continue
            # If we know the available fields, skip sending fields that Airtable doesn't have,
            # UNLESS it is explicitly in force_update_fields.
            if self._available_fields is not None and k not in self._available_fields and k not in force_update_fields:
                continue
            fields[k] = v
            
        if force_update_fields:
            self.logger.debug(f"Forcing update for fields: {force_update_fields} on {booking.get('booking_nr')}")

        try:
            import json as _json
            non_null_keys = [k for k, v in full_fields.items() if v is not None]
            missing_required = [k for k in self._required_fields if full_fields.get(k) is None]
            with open(self.payload_debug_file, "a", encoding="utf-8") as pf:
                pf.write(_json.dumps({
                    "booking_nr": booking.get("booking_nr"),
                    "prepared_fields": fields,
                    "non_null_keys": non_null_keys,
                    "missing_required": missing_required,
                    "full_fields": full_fields
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        try:
            params = {"filterByFormula": f"{{Booking Nr.}}='{booking.get('booking_nr')}'"}
            
            # --- 1. CHECK MIRROR BASE FIRST ---
            # If the mirror base matches the new fields, it means there are no new changes from GYG.
            # We skip the main base update to preserve manual edits in the main base.
            if self.mirror_api_url:
                mirror_find = requests.get(self.mirror_api_url, headers=headers, params=params, timeout=30)
                
                if mirror_find.status_code == 429:
                    self.logger.warning(f"Rate limit hit when checking Mirror Base for {booking.get('booking_nr')}. Waiting 2 seconds.")
                    import time
                    time.sleep(2)
                    mirror_find = requests.get(self.mirror_api_url, headers=headers, params=params, timeout=30)
                    
                if mirror_find.status_code == 200:
                    m_data = mirror_find.json() or {}
                    m_records = m_data.get("records") or []
                    if m_records:
                        m_rid = m_records[0].get("id")
                        existing_m_record = m_records[0].get("fields", {})
                        
                        # Compare incoming fields with mirror base fields
                        is_identical = True
                        changed_fields = {}
                        for k, v in fields.items():
                            old_val = existing_m_record.get(k)
                            new_val = v
                            
                            # Normalize for comparison
                            if old_val is None:
                                old_val = ""
                            if new_val is None:
                                new_val = ""
                                
                            str_old = str(old_val).strip()
                            str_new = str(new_val).strip()
                            
                            # Skip empty new values so we don't force updates for missing extractions
                            if not str_new or str_new == "None":
                                continue
                                
                            # Special handling for arrays/lists (like Add - Ons, Options)
                            if isinstance(new_val, list) or isinstance(old_val, list):
                                try:
                                    import json
                                    list_old = old_val if isinstance(old_val, list) else (json.loads(old_val) if old_val else [])
                                    list_new = new_val if isinstance(new_val, list) else (json.loads(new_val) if new_val else [])
                                    # Convert to sorted strings for comparison to ignore order
                                    if sorted([str(x) for x in list_old]) == sorted([str(x) for x in list_new]):
                                        continue
                                    else:
                                        self.logger.debug(f"Mirror mismatch on {k} (List): {list_old} != {list_new}")
                                        changed_fields[k] = new_val
                                        is_identical = False
                                        continue
                                except Exception:
                                    pass

                            # Handle strings that look like semicolon or comma separated lists (like Add - Ons text)
                            if k == "Add - Ons":
                                # Sometimes separated by ';' and sometimes by ',' depending on the source
                                addons_old = sorted([x.strip() for x in str_old.replace(',', ';').split(';')]) if str_old else []
                                addons_new = sorted([x.strip() for x in str_new.replace(',', ';').split(';')]) if str_new else []
                                
                                # Clean up common prefix like '2 x ' or '1 x ' before comparison
                                import re
                                addons_old = sorted([re.sub(r'^\d+\s*x\s*', '', x) for x in addons_old if x])
                                addons_new = sorted([re.sub(r'^\d+\s*x\s*', '', x) for x in addons_new if x])
                                
                                if addons_old == addons_new:
                                    continue
                                else:
                                    self.logger.debug(f"Mirror mismatch on {k} (String List): {addons_old} != {addons_new}")
                                    changed_fields[k] = new_val
                                    is_identical = False
                                    continue

                            # Special handling for dates (compare only the first 10 chars YYYY-MM-DD if applicable)
                            if k in ["Date Trip", "Real Date Trip"] and len(str_old) >= 16 and len(str_new) >= 16:
                                # Sometimes Airtable appends 'Z' to floating times in the API response
                                # We only want to compare the actual numbers YYYY-MM-DDTHH:MM
                                str_old_time = str_old[:16]
                                str_new_time = str_new[:16]
                                if str_old_time == str_new_time:
                                    continue
                                else:
                                    changed_fields[k] = new_val
                                    is_identical = False
                                    self.logger.debug(f"Mirror mismatch on {k} (Date/Time): {str_old_time} != {str_new_time}")
                                    continue
                            elif k in ["Date Trip", "Real Date Trip"] and len(str_old) >= 10 and len(str_new) >= 10:
                                # Fallback to Date only comparison if time is missing
                                if str_old[:10] == str_new[:10]:
                                    continue
                                else:
                                    changed_fields[k] = new_val
                                    is_identical = False
                                    self.logger.debug(f"Mirror mismatch on {k} (Date): {str_old[:10]} != {str_new[:10]}")
                                    continue
                                    
                            # Special handling for floats/numbers (even if they are strings)
                            is_num_new = False
                            is_num_old = False
                            f_new = 0.0
                            f_old = 0.0
                            
                            try:
                                if str_new != "":
                                    f_new = float(str_new)
                                    is_num_new = True
                            except ValueError:
                                pass
                                
                            try:
                                if str_old != "":
                                    f_old = float(str_old)
                                    is_num_old = True
                            except ValueError:
                                pass

                            if is_num_new and is_num_old:
                                if abs(f_old - f_new) > 0.001:
                                    changed_fields[k] = new_val
                                    is_identical = False
                                    self.logger.debug(f"Mirror mismatch on {k}: {f_old} != {f_new}")
                                continue
                            elif is_num_new != is_num_old and (str_new != "" and str_old != ""):
                                # One is a number, the other is not (e.g. string with text)
                                pass # Fallback to string comparison
                                    
                            if str_new != str_old:
                                # We only consider it a mismatch if it's not a missing value issue
                                changed_fields[k] = new_val
                                is_identical = False
                                self.logger.debug(f"Mirror mismatch on {k}: Old='{str_old}' ({type(old_val)}) != New='{str_new}' ({type(new_val)})")
                                
                        if is_identical:
                            self.logger.info(f"Skipping Main Base update for {booking.get('booking_nr')} - Matches Mirror Base perfectly (No new changes from GYG).")
                            return {"success": True, "record_id": m_rid, "skipped": True}
                        else:
                            for f_forced in force_update_fields:
                                if f_forced in fields:
                                    changed_fields[f_forced] = fields[f_forced]
                            
                            self.logger.info(f"Delta detected for {booking.get('booking_nr')}. Only updating changed fields: {list(changed_fields.keys())}")
                            fields = changed_fields
                    else:
                        self.logger.info(f"Record {booking.get('booking_nr')} not found in Mirror Base. Proceeding with full update.")
                else:
                    self.logger.error(f"Failed to fetch Mirror Base for {booking.get('booking_nr')}. Status Code: {mirror_find.status_code}. Response: {mirror_find.text}")
            # ----------------------------------

            
            find = requests.get(self.api_url, headers=headers, params=params, timeout=30)
            if find.status_code == 200:
                data = find.json() or {}
                records = data.get("records") or []
                if records:
                    rid = records[0].get("id")
                    
                    update_fields = fields.copy()
                    
                    self.logger.info(f"Patching Airtable {booking.get('booking_nr')} with fields: {list(update_fields.keys())}")
                        
                    patch = requests.patch(f"{self.api_url}/{rid}", headers=headers, json={"fields": update_fields, "typecast": True}, timeout=30)
                    
                    # --- 2. UPDATE MIRROR BASE ON PATCH ---
                    if self.mirror_api_url and patch.status_code in (200, 201):
                        self._sync_to_mirror_base(booking.get('booking_nr'), fields, headers)
                    # --------------------------------------
                    
                    try:
                        import json as _json
                        with open(self.test_file, "a", encoding="utf-8") as f:
                            f.write(_json.dumps({
                                "booking_nr": booking.get("booking_nr"),
                                "action": "patch",
                                "code": patch.status_code,
                                "record_id": rid,
                                "fields_sent": fields,
                                "fields_all": full_fields
                            }, ensure_ascii=False) + "\n")
                        missing = [k for k in full_fields.keys() if k not in fields and full_fields.get(k) is not None and (self._available_fields is None or k in self._available_fields)]
                        nulls = [k for k, v in full_fields.items() if v is None]
                        unavailable = [k for k in full_fields.keys() if (self._available_fields is not None and k not in self._available_fields)]
                        with open(self.report_file, "a", encoding="utf-8") as rf:
                            rf.write(_json.dumps({
                                "booking_nr": booking.get("booking_nr"),
                                "action": "patch",
                                "code": patch.status_code,
                                "record_id": rid,
                                "sent_keys": list(fields.keys()),
                                "missing_keys": missing,
                                "null_keys": nulls,
                                "unavailable_columns": unavailable
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    if patch.status_code == 422:
                        self.logger.warning(f"Airtable PATCH 422 for {booking.get('booking_nr')}. Possible schema mismatch or missing column.")
                    
                    if patch.status_code in (200, 201):
                        return {"success": True, "record_id": rid}
                    return {"success": False, "code": patch.status_code}
            create = requests.post(self.api_url, headers=headers, json={"fields": fields, "typecast": True}, timeout=30)
            try:
                import json as _json
                rid_create = (create.json() or {}).get("id") if create.status_code in (200,201) else None
                with open(self.test_file, "a", encoding="utf-8") as f:
                    f.write(_json.dumps({
                        "booking_nr": booking.get("booking_nr"),
                        "action": "create",
                        "code": create.status_code,
                        "record_id": rid_create,
                        "fields_sent": fields,
                        "fields_all": full_fields,
                        "error_body": (create.text if create.status_code not in (200,201) else None)
                    }, ensure_ascii=False) + "\n")
                missing = [k for k in full_fields.keys() if k not in fields and full_fields.get(k) is not None and (self._available_fields is None or k in self._available_fields)]
                nulls = [k for k, v in full_fields.items() if v is None]
                unavailable = [k for k in full_fields.keys() if (self._available_fields is not None and k not in self._available_fields)]
                with open(self.report_file, "a", encoding="utf-8") as rf:
                    rf.write(_json.dumps({
                        "booking_nr": booking.get("booking_nr"),
                        "action": "create",
                        "code": create.status_code,
                        "record_id": rid_create,
                        "sent_keys": list(fields.keys()),
                        "missing_keys": missing,
                        "null_keys": nulls,
                        "unavailable_columns": unavailable
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            if create.status_code in (200, 201):
                rid = create.json().get("id")
                # --- 3. CREATE IN MIRROR BASE ---
                if self.mirror_api_url:
                    self._sync_to_mirror_base(booking.get('booking_nr'), fields, headers)
                # --------------------------------
                return {"success": True, "record_id": rid}
            # Fallback: attempt minimal record creation if schema is empty or columns are missing
            if create.status_code == 422:
                minimal_candidates = [
                    ("Booking Nr.", booking.get("booking_nr")),
                    ("trip Name", booking.get("trip_name") or booking.get("destination")),
                    ("Customer Name", booking.get("customer_name")),
                    ("Name", booking.get("booking_nr") or booking.get("trip_name") or booking.get("customer_name")),
                ]
                minimal = {}
                for k, v in minimal_candidates:
                    if v:
                        minimal[k] = v
                        break
                if minimal:
                    c2 = requests.post(self.api_url, headers=headers, json={"fields": minimal, "typecast": True}, timeout=30)
                    try:
                        import json as _json
                        rid2 = (c2.json() or {}).get("id") if c2.status_code in (200,201) else None
                        with open(self.test_file, "a", encoding="utf-8") as f:
                            f.write(_json.dumps({
                                "booking_nr": booking.get("booking_nr"),
                                "action": "create_fallback_minimal",
                                "code": c2.status_code,
                                "record_id": rid2,
                                "fields_sent": minimal,
                                "error_body": (c2.text if c2.status_code not in (200,201) else None)
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    if c2.status_code in (200, 201):
                        rid = c2.json().get("id")
                        # --- 4. CREATE MINIMAL IN MIRROR BASE ---
                        if self.mirror_api_url:
                            self._sync_to_mirror_base(booking.get('booking_nr'), minimal, headers)
                        # ----------------------------------------
                        return {"success": True, "record_id": rid}
            return {"success": False, "code": create.status_code}
        except Exception as e:
            return {"success": False, "error": str(e)}

class GYGUnifiedSystem:
    def __init__(self):
        self.logger = setup_logging()
        self.email = os.getenv("GYG_EMAIL")
        self.password = os.getenv("GYG_PASSWORD")
        self.totp_secret = os.getenv("GYG_2FA_SECRET")
        self.managed_by = os.getenv("GYG_MANAGED_BY")
        self.sync_interval = int(os.getenv("SYNC_INTERVAL_MINUTES", 5))
        self.restart_delay_minutes = int(os.getenv("RESTART_DELAY_MINUTES", 5))
        self.auto_sync = os.getenv("AUTO_SYNC_ENABLED", "true").lower() == "true"
        self.headless = os.getenv("HEADLESS_MODE", "false").lower() == "true" or os.getenv("BROWSER_HEADLESS", "true").lower() == "true"
        self.run_once_flag = False
        self.max_pages = int(os.getenv("MAX_PAGES", 100)) # Default to 100 if not set
        self.db = BookingsDatabase(logger=self.logger)
        self.airtable = AirtableManager(logger=self.logger)
        self.deepseek_key = os.getenv("DEEPSEEK_API_KEY")
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.session_counter = 0
        self.max_session_time = 60
        self.current_page = 1
        self.persistent = os.getenv("BROWSER_PERSISTENT", "true").lower() == "true"
        self.user_data_dir = os.getenv("BROWSER_USER_DATA_DIR", str(Path(__file__).parent / "browser_profile"))
        self.engine = os.getenv("BROWSER_ENGINE", "chromium").lower()
        self.channel = os.getenv("BROWSER_CHANNEL", "")
        self.storage_state_path = os.getenv("BROWSER_STORAGE_STATE", str(Path(__file__).parent / "browser_state.json"))
        self.max_retries = int(os.getenv("RECOVERY_MAX_RETRIES", 3))
        self.retry_backoff_sec = int(os.getenv("RECOVERY_BACKOFF_SEC", 5))
        self.failure_count = 0
        self.SELECTORS = {
            "booking_card": '[data-testid="booking-card"]',
            "booking_reference": '[data-testid="booking-reference"]',
            "show_details_btn": 'button:has-text("Show details")',
            "show_breakdown_btn": 'button:has-text("Show breakdown")',
            "lead_traveler": '[data-testid="lead-traveler-name"]',
            "lead_phone": '[data-testid="lead-traveler-phone"]',
            "breakdown_table": 'table',
            "breakdown_total_row": 'tfoot tr',
            "activity_title": '[data-testid="booking-card-activity-title"]',
            "option_text": 'p.text-caption.text-label-secondary',
            "activity_date": 'p[data-testid="conduction-time"]',
            "participants": '[data-testid="participants-and-price"]',
            "google_maps": 'a:has-text("Open in google maps")',
            "guide_language": '[data-testid="booking-detail-conduction-language"]',
        }

    async def _handle_overlays(self):
        try:
            btn = await self.page.query_selector('button:has-text("I agree")')
            if not btn:
                btn = await self.page.query_selector('button:has-text("Only essential")')
            if btn:
                await btn.click()
                await asyncio.sleep(0.3)
            else:
                for fr in self.page.frames:
                    try:
                        btnf = await fr.query_selector('button:has-text("I agree")')
                        if not btnf:
                            btnf = await fr.query_selector('button:has-text("Only essential")')
                        if btnf:
                            await btnf.click()
                            await asyncio.sleep(0.3)
                            break
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            chat = await self.page.query_selector('text=Support chat')
            if chat:
                close_btn = await self.page.query_selector('button[aria-label*="close" i], button[aria-label*="minimize" i]')
                if close_btn:
                    await close_btn.click()
                    await asyncio.sleep(0.3)
                else:
                    svg = await self.page.query_selector('svg[data-garden-id="buttons.icon"]')
                    if svg:
                        try:
                            await svg.evaluate('(el)=>{const b=el.closest("button"); if(b){b.click();}}')
                            await asyncio.sleep(0.3)
                        except Exception:
                            pass
                # Fallback: force hide by CSS
                try:
                    await self.page.evaluate('() => { const el = Array.from(document.querySelectorAll("div,section,iframe"))\n                        .find(e => (e.textContent||"").includes("Support chat"));\n                        if (el) { el.style.display = "none"; } }')
                except Exception:
                     pass
            if not chat:
                for fr in self.page.frames:
                    try:
                        node = await fr.query_selector('svg[data-garden-id="buttons.icon"]')
                        if node:
                            await node.evaluate('(el)=>{const b=el.closest("button"); if(b){b.click();}}')
                            await asyncio.sleep(0.3)
                            break
                        # Fallback CSS hide inside frame
                        await fr.evaluate('() => { const el = Array.from(document.querySelectorAll("div,section,iframe"))\n                            .find(e => (e.textContent||"").includes("Support chat"));\n                            if (el) { el.style.display = "none"; } }')
                    except Exception:
                        pass
        except Exception:
            pass

    async def close_support_chat_if_open(self):
        try:
            close_btn = await self.page.query_selector('button[aria-label*="close" i], button[aria-label*="minimize" i]')
            if close_btn:
                await close_btn.click()
                await asyncio.sleep(0.2)
                return
            svg = await self.page.query_selector('svg[data-garden-id="buttons.icon"]')
            if svg:
                try:
                    await svg.evaluate('(el)=>{const b=el.closest("button"); if(b){b.click();}}')
                    await asyncio.sleep(0.2)
                    return
                except Exception:
                    pass
            await self.page.evaluate('() => { const el = Array.from(document.querySelectorAll("div,section,iframe"))\n                .find(e => (e.textContent||"").includes("Support chat")); if (el) { el.style.display = "none"; }}')
        except Exception:
            pass

    async def suppress_chat_widgets(self):
        try:
            await self.page.evaluate("""
                () => {
                    document.querySelectorAll('iframe').forEach(iframe => {
                        const title = (iframe.title || '').toLowerCase();
                        if (title.includes('message') || title.includes('support') || title.includes('chat')) {
                            iframe.remove();
                        }
                    });
                    document.querySelectorAll('button').forEach(btn => {
                        const t = (btn.textContent || '').toLowerCase();
                        const idt = (btn.id || '').toLowerCase();
                        if (t.includes('help') || idt.includes('conversation')) {
                            btn.remove();
                        }
                    });
                }
            """)
        except Exception:
            pass
        try:
            await self.close_support_chat_if_open()
        except Exception:
            pass

    async def initialize_browser(self) -> bool:
        try:
            from playwright.async_api import async_playwright
            self.playwright = await async_playwright().start()
            args = ["--disable-blink-features=AutomationControlled"]
            if not self.headless:
                args.append("--start-maximized")
            
            # Use a realistic User-Agent to avoid detection in headless mode
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            
            context = None
            if self.engine == "chromium":
                if self.persistent:
                    kw = {"headless": self.headless, "args": args, "user_agent": user_agent}
                    if self.channel:
                        if self.headless:
                            try:
                                args.append("--headless=new")
                            except Exception:
                                pass
                        kw["channel"] = self.channel
                    try:
                        self.context = await self.playwright.chromium.launch_persistent_context(self.user_data_dir, **kw)
                        context = self.context
                    except Exception as e:
                        try:
                            self.logger.warning(f"Chromium persistent launch failed with channel '{self.channel}': {e}")
                        except Exception:
                            pass
                        try:
                            kw.pop("channel", None)
                            self.context = await self.playwright.chromium.launch_persistent_context(self.user_data_dir, **kw)
                            context = self.context
                        except Exception as e2:
                            try:
                                self.logger.error(f"Chromium persistent launch fallback failed: {e2}")
                            except Exception:
                                pass
                            raise e2
                else:
                    kw = {"headless": self.headless, "args": args, "user_agent": user_agent}
                    if self.channel:
                        if self.headless:
                            try:
                                args.append("--headless=new")
                            except Exception:
                                pass
                        kw["channel"] = self.channel
                    try:
                        self.browser = await self.playwright.chromium.launch(**kw)
                    except Exception as e:
                        try:
                            self.logger.warning(f"Chromium launch failed with channel '{self.channel}': {e}")
                        except Exception:
                            pass
                        try:
                            kw.pop("channel", None)
                            self.browser = await self.playwright.chromium.launch(**kw)
                        except Exception as e2:
                            try:
                                self.logger.error(f"Chromium launch fallback failed: {e2}")
                            except Exception:
                                pass
                            raise e2
                    if os.path.exists(self.storage_state_path):
                        try:
                            context = await self.browser.new_context(storage_state=self.storage_state_path)
                        except Exception as e:
                            self.logger.warning(f"Failed to load storage state (corrupt?): {e}. Starting fresh.")
                            context = await self.browser.new_context()
                    else:
                        context = await self.browser.new_context()
            elif self.engine == "firefox":
                self.browser = await self.playwright.firefox.launch(headless=self.headless)
                if os.path.exists(self.storage_state_path):
                    try:
                        context = await self.browser.new_context(storage_state=self.storage_state_path)
                    except Exception as e:
                        self.logger.warning(f"Failed to load storage state (corrupt?): {e}. Starting fresh.")
                        context = await self.browser.new_context()
                else:
                    context = await self.browser.new_context()
                self.context = context
            else:
                self.browser = await self.playwright.webkit.launch(headless=self.headless)
                if os.path.exists(self.storage_state_path):
                    try:
                        context = await self.browser.new_context(storage_state=self.storage_state_path)
                    except Exception as e:
                        self.logger.warning(f"Failed to load storage state (corrupt?): {e}. Starting fresh.")
                        context = await self.browser.new_context()
                else:
                    context = await self.browser.new_context()
                self.context = context
            async def _chat_blocker(route):
                try:
                    u = (route.request.url or "").lower()
                    if any(k in u for k in ["intercom","zendesk","livechat","drift","crisp","tawk","messaging-widget"]):
                        await route.abort()
                    else:
                        await route.continue_()
                except Exception:
                    try:
                        await route.continue_()
                    except Exception:
                        pass
            await context.route("**/*", _chat_blocker)
            self.page = await context.new_page()
            try:
                await self.page.add_style_tag(content="""
                    iframe[title*="Messaging"], iframe[title*="Support"],
                    button:has-text("Help"), [id*="conversation"],
                    [class*="chat-widget"], [class*="support-widget"] {
                        display: none !important;
                        visibility: hidden !important;
                    }
                """)
            except Exception:
                pass
            self.logger.info("Browser initialized with chat blocking enabled")
            return True
        except Exception as e:
            self.logger.error(f"Browser initialization failed: {e}")
            return False

    async def _navigate_to_login(self, allow_restart: bool) -> bool:
        try:
            await self.page.goto("https://supplier.getyourguide.com/auth/login", wait_until="domcontentloaded", timeout=60000)
            try:
                await self.page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            await asyncio.sleep(2)
            
            body_len = await self.page.evaluate("() => document.body.innerText.length")
            if body_len < 50:
                self.logger.warning("Detected potential white screen on login. Reloading...")
                await self.page.reload(wait_until="domcontentloaded")
                await asyncio.sleep(3)
                if await self.page.evaluate("() => document.body.innerText.length") < 50:
                    self.logger.warning("Reload failed to fix white screen. Recreating page...")
                    if self.page:
                        try:
                            await self.page.close()
                        except Exception:
                            pass
                    if self.context:
                        self.page = await self.context.new_page()
                        try:
                            await self.page.goto("https://supplier.getyourguide.com/auth/login", wait_until="domcontentloaded", timeout=60000)
                        except Exception:
                            pass
                        await asyncio.sleep(2)
            return True
        except Exception as e:
            if allow_restart and "collected" in str(e).lower() and "heap" in str(e).lower():
                self.logger.error("Critical: Page object collected (memory issue). Forcing restart.")
                await self._restart_from_beginning("login_page_collected")
                return False
            return True

    async def _check_already_logged_in(self) -> bool:
        if await self.page.query_selector('text="Analytics"') or \
           await self.page.query_selector('text="Revenue"') or \
           await self.page.query_selector('text="Bookings"') or \
           "bookings" in self.page.url:
             self.logger.info("Already logged in (Home/Analytics/Bookings detected). Navigating to Bookings...")
             if "bookings" not in self.page.url:
                 url = "https://supplier.getyourguide.com/bookings"
                 if self.managed_by:
                     url += f"?managed_by={self.managed_by}"
                 await self.page.goto(url, wait_until="domcontentloaded")
                 await asyncio.sleep(2)
             
             self.session_counter = 0
             if self.context and not self.persistent:
                 try:
                     await self.context.storage_state(path=self.storage_state_path)
                     self.logger.info("Session state saved (already logged in).")
                 except Exception:
                     pass
             return True
        return False

    async def _fill_credentials(self, allow_restart: bool) -> bool:
        try:
            email_el = await self.page.query_selector('input[type="email"]')
            pass_el = await self.page.query_selector('input[type="password"]')
            if not email_el or not pass_el:
                if await self._check_already_logged_in():
                    return True
                self.logger.error("Login form elements not found")
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                await self.page.screenshot(path=f"login_form_missing_{ts}.png")
                return False
                
            await email_el.fill(self.email or "")
            await asyncio.sleep(0.5)
            await pass_el.fill(self.password or "")
            await asyncio.sleep(0.5)
            login_btn = await self.page.query_selector('button[type="submit"]')
            if login_btn:
                await login_btn.click()
            await asyncio.sleep(3)
            return True
        except Exception as e:
            if allow_restart and "collected" in str(e).lower() and "heap" in str(e).lower():
                self.logger.error("Critical: Page object collected during element check. Forcing restart.")
                await self._restart_from_beginning("login_element_check_collected")
            return False

    async def _handle_login_errors(self, allow_restart: bool) -> bool:
        error_msg = await self.page.query_selector('.error-message, [role="alert"]')
        if error_msg:
            txt = await error_msg.text_content()
            self.logger.error(f"Login error message: {txt}")
            if "error with your session" in txt.lower() or "log you out" in txt.lower():
                self.logger.critical("Corrupt session detected. Clearing saved state and retrying fresh.")
                try:
                    await self._clear_browser_data()
                    self.logger.info("Cleared browser cookies, storage state, and profile data")
                    if allow_restart:
                        return await self._restart_from_beginning("corrupt_session")
                except Exception as e:
                    self.logger.error(f"Failed to clear corrupt session: {e}")
                return True
        return False

    async def _handle_totp(self) -> None:
        if not self.totp_secret:
            return
        code = pyotp.TOTP(self.totp_secret).now()
        code_inputs = await self.page.query_selector_all('input[type="text"], input[type="tel"], input[type="number"], input[inputmode="numeric"]')
        
        if len(code_inputs) >= 6:
            for i in range(6):
                if i < len(code):
                    await code_inputs[i].fill(code[i])
                    await asyncio.sleep(0.1)
        elif len(code_inputs) == 1:
            await code_inputs[0].fill(code)
        else:
            if code_inputs:
                await code_inputs[0].fill(code)

        verify_btn = await self.page.query_selector('button:has-text("Verify code")')
        if verify_btn:
            await verify_btn.click()
        else:
            await self.page.press('input[type="text"]', 'Enter')
        await asyncio.sleep(3)
        
        try:
            error_banner = await self.page.query_selector('text=verification code is either incorrect or has expired')
            if error_banner:
                self.logger.warning("OTP incorrect/expired. Waiting 30s for new code...")
                await asyncio.sleep(30)
                new_code = pyotp.TOTP(self.totp_secret).now()
                if new_code == code:
                     await asyncio.sleep(5)
                     new_code = pyotp.TOTP(self.totp_secret).now()
                
                if len(code_inputs) >= 6:
                    for i in range(6):
                        if i < len(new_code):
                            await code_inputs[i].fill(new_code[i])
                            await asyncio.sleep(0.1)
                elif len(code_inputs) == 1:
                    await code_inputs[0].fill(new_code)
                    
                if verify_btn:
                    await verify_btn.click()
                else:
                    await self.page.press('body', 'Enter')
                await asyncio.sleep(3)
        except Exception:
            pass

    async def _verify_login_success(self) -> bool:
        await self.page.wait_for_timeout(3000)
        otp_field = await self.page.query_selector('input[type="text"], input[type="tel"], input[type="number"], input[inputmode="numeric"]')
        if otp_field:
            self.logger.info("OTP field detected. Processing 2FA...")
            await self._handle_totp()

        if await self._check_already_logged_in():
            return True

        if await self.page.query_selector('button[type="submit"]'):
            self.logger.error("Login failed - still on login page")
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            await self.page.screenshot(path=f"login_failed_stuck_{ts}.png")
            return False
            
        self.session_counter = 0
        if self.context and not self.persistent:
            try:
                await self.context.storage_state(path=self.storage_state_path)
                self.logger.info("Session state saved (new login).")
            except Exception:
                pass
        return True

    async def login(self, allow_restart: bool = True) -> bool:
        try:
            if not await self._navigate_to_login(allow_restart):
                return False
                
            try:
                await self.suppress_chat_widgets()
            except Exception:
                pass
            
            if await self.page.query_selector('iframe[src*="recaptcha"], iframe[src*="cloudflare"]'):
                self.logger.warning("CAPTCHA detected on login page")
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                await self.page.screenshot(path=f"captcha_detected_{ts}.png")
            
            if await self._check_already_logged_in():
                return True
                
            if not await self._fill_credentials(allow_restart):
                return False
                
            if await self._handle_login_errors(allow_restart):
                return False
                
            await self._handle_totp()
            
            try:
                rate_limit = await self.page.query_selector('text=exceeded the allowed number of requests')
                if rate_limit:
                    self.logger.critical("LOGIN BLOCKED: Rate limit exceeded (too many 2FA attempts). Pausing execution for 30 minutes.")
                    for i in range(30):
                         if i % 5 == 0:
                             self.logger.info(f"Cooling down... {30-i} minutes remaining.")
                         await asyncio.sleep(60)
                    return False
            except Exception:
                pass

            return await self._verify_login_success()
            
        except Exception as e:
            self.logger.error(f"Login exception: {str(e)}")
            if allow_restart and "collected" in str(e).lower() and "heap" in str(e).lower():
                self.logger.error("Critical: Page object collected (memory issue) in main login block. Forcing restart.")
                await self._restart_from_beginning("login_exception_collected")
            try:
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                if self.page:
                    await self.page.screenshot(path=f"login_exception_{ts}.png")
            except Exception:
                pass
            return False

    async def check_session(self) -> bool:
        try:
            try:
                ok = await self._ensure_page_responsive()
            except Exception:
                ok = False
            if not ok:
                try:
                    await self._recover_from_failure("page_unresponsive")
                except Exception:
                    pass
            
            # Additional check for blank page
            try:
                body_len = await self.page.evaluate("() => document.body.innerText.length")
                if body_len < 50:
                    # Page is effectively empty/white, assume session lost or page broken
                    self.logger.warning("Page content empty in check_session. Triggering login.")
                    return await self.login()
            except Exception:
                pass
                
            # if self.session_counter > self.max_session_time:
            #    self.session_counter = 0
            #    return await self.login()
            
            login_btn = await self.page.query_selector('button[type="submit"]')
            email_visible = await self.page.is_visible('input[type="email"]') if login_btn else False
            if login_btn and email_visible:
                return await self.login()
            
            self.session_counter += 5
            return True
        except Exception:
            # Only trigger login if we are truly broken
            return False # Let the main loop handle recovery instead of forcing login inside check

    async def fetch_details_from_subpage(self, url: str) -> Dict:
        """Fetch email and other details from the booking sub-page."""
        res = {"email": None, "phone": None, "adt": None, "std": None, "chd": None, "inf": None, "youth": None, "add_ons": None}
        new_page = None
        
        # Retry loop for page loading (max 3 attempts)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not url:
                    return res
                full_url = url if url.startswith('http') else f"https://supplier.getyourguide.com{url}"
                
                # Capture current session storage
                ss_data = "{}"
                try:
                    if self.page:
                        ss_data = await self.page.evaluate("() => JSON.stringify(sessionStorage)")
                except Exception:
                    pass

                # Close previous attempt page if it exists
                if new_page:
                    try:
                        await new_page.close()
                    except:
                        pass
                
                # Create a new page
                new_page = await self.context.new_page()
                
                # Inject session storage
                if ss_data and ss_data != "{}":
                    js_injector = f"""
                        try {{
                            const data = {ss_data};
                            for (const key in data) {{
                                sessionStorage.setItem(key, data[key]);
                            }}
                        }} catch (e) {{ }}
                    """
                    await new_page.add_init_script(js_injector)
                
                # Navigate with delay simulation
                await asyncio.sleep(2) # Simulate human delay before navigation
                await new_page.goto(full_url, wait_until="domcontentloaded", timeout=30000)
                
                # Wait for content to fully load
                try:
                    await asyncio.sleep(3)
                except Exception:
                    pass
                
                # Check for "Something went wrong" error page
                try:
                    body_text = await new_page.evaluate("document.body.innerText")
                    if "Something went wrong" in body_text:
                        self.logger.warning(f"Error page detected for {url} (Attempt {attempt+1}/{max_retries}). Retrying...")
                        await asyncio.sleep(2)
                        continue # Retry loop
                except Exception:
                    pass
                
                # If we get here, the page loaded successfully (no error text found)
                break 
                
            except Exception as e:
                self.logger.error(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt == max_retries - 1:
                    pass # Continue to cleanup
                await asyncio.sleep(2)

        if not new_page:
             return res

        try:
            # AI Enhancement Step 1: Get full page text for DeepSeek
            page_text_content = ""
            try:
                page_text_content = await new_page.evaluate("document.body.innerText")
            except Exception:
                pass

            if self.deepseek_key and page_text_content:
                try:
                    self.logger.info(f"Invoking DeepSeek AI for details extraction on {url}...")
                    ai_res = ai_enhance(self.deepseek_key, page_text_content, "", {})
                    
                    # Merge AI results
                    if ai_res.get("add_ons"):
                        res["add_ons"] = ai_res.get("add_ons")
                        self.logger.info(f"AI extracted Add-Ons: {res['add_ons']}")
                    
                    # Merge participants if AI found them and they look valid
                    for k in ["adt", "std", "chd", "inf", "youth"]:
                        if ai_res.get(k) is not None:
                            res[k] = ai_res.get(k)
                            
                    # Merge email/phone if found
                    if ai_res.get("customer_email"):
                        res["email"] = ai_res.get("customer_email")
                    if ai_res.get("customer_phone"):
                        res["phone"] = ai_res.get("customer_phone")
                        
                except Exception as e:
                    self.logger.error(f"AI enhancement failed on subpage: {e}")

            # Continue with regex/selector fallbacks (existing logic)
            
            # Try to wait for the specific element provided by user
            try:
                await new_page.wait_for_selector('li[data-testid="booking-detail-lead-traveler"], [data-testid="lead-traveler-email"], a[href^="mailto:"]', timeout=5000)
            except Exception:
                pass

            # 1. Broad search for ANY mailto link
            try:
                mailto_links = await new_page.query_selector_all('a[href^="mailto:"]')
                for link in mailto_links:
                    href = await link.get_attribute('href')
                    if href:
                        clean_email = href.replace('mailto:', '').strip()
                        if clean_email and '@' in clean_email:
                            res["email"] = clean_email
                            break
            except Exception:
                pass

            # 2. Try specific selector provided by user for Email (if not found yet)
            if not res["email"]:
                try:
                    email_el = await new_page.query_selector('a[data-testid="lead-traveler-email"]')
                    if email_el:
                        txt = await email_el.text_content()
                        if txt and '@' in txt:
                            res["email"] = txt.strip()
                except Exception:
                    pass

            # 3. Search for "Email" label and following text
            if not res["email"]:
                try:
                    # Look for elements containing "Email" and check their siblings/children
                    email_labels = await new_page.query_selector_all('text=/Email/i')
                    for label in email_labels:
                        # Check next sibling
                        try:
                            sibling_text = await label.evaluate("el => el.nextElementSibling ? el.nextElementSibling.textContent : ''")
                            if sibling_text and '@' in sibling_text:
                                res["email"] = sibling_text.strip()
                                break
                            # Check parent's text
                            parent_text = await label.evaluate("el => el.parentElement ? el.parentElement.textContent : ''")
                            emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', parent_text)
                            if emails:
                                res["email"] = emails[0]
                                break
                        except:
                            pass
                except Exception:
                    pass

            # 4. Regex fallback on full page text (most aggressive)
            if not res["email"]:
                try:
                    # Get innerText which is better for visual text
                    content = await new_page.evaluate("document.body.innerText")
                    # Regex for email: standard + GYG proxy (case insensitive)
                    # Improve regex to capture all valid email characters
                    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', content)
                    
                    # Filter and prioritize emails starting with 'customer' (GYG proxy format)
                    valid_emails = []
                    for em in emails:
                        em_lower = em.lower()
                        # Explicitly filter out customer care and internal non-reply emails
                        if "customer.care" in em_lower:
                            continue
                        if "getyourguide.com" in em_lower and "reply" not in em_lower:
                            continue
                        # Filter out common false positives
                        if em_lower in ["email@email.com", "name@example.com"]:
                            continue
                        valid_emails.append(em)

                    customer_emails = [em for em in valid_emails if em.lower().startswith('customer')]
                    
                    if customer_emails:
                        res["email"] = customer_emails[0]
                    elif valid_emails:
                        res["email"] = valid_emails[0]
                        
                except Exception:
                    pass

            if not res["email"]:
                self.logger.warning(f"Email still missing for {url} after all attempts.")
                # Optional: Log a snippet of text to see what was there
                try:
                     preview = (await new_page.evaluate("document.body.innerText"))[:500]
                     self.logger.info(f"Page text preview: {preview.replace(chr(10), ' ')}")
                except:
                     pass

            # Phone extraction improvements
            try:
                # 1. Try tel link first
                tel_links = await new_page.query_selector_all('a[href^="tel:"]')
                for link in tel_links:
                     href = await link.get_attribute('href')
                     if href:
                         res["phone"] = href.replace('tel:', '').strip()
                         break
                
                if not res["phone"]:
                    content = await new_page.evaluate("document.body.innerText")
                    # Look for phone pattern specifically near "Phone" or "Mobile"
                    # Or just general phone regex
                    phones = re.findall(r'(?:\+|00)\d[\d\s-]{7,}', content)
                    if phones:
                        res["phone"] = phones[0].strip()
            except Exception:
                pass
            
            # Extract participants breakdown from details page
            # Re-initialize counts to ensure accuracy from details page if available
            try:
                # Look for the participants list items
                items_el = await new_page.query_selector_all('[data-testid="participants-breakdown-items"] li')
                if items_el:
                    # Reset counts if we found the details section
                    res["adt"] = 0; res["std"] = 0; res["chd"] = 0; res["inf"] = 0; res["youth"] = 0
                    extras = []
                    
                    for it in items_el:
                        t = (await it.text_content() or '').strip()
                        t_lower = t.lower()
                        
                        # Match participant counts
                        # Match number at start of string: "2 Adults..." -> 2
                        nm = re.search(r'^(\d+)', t)
                        if nm:
                            v = int(nm.group(1))
                            
                            # Filter out unreasonable counts (e.g. 100 adults) which likely come from add-on prices (e.g. "100.00")
                            # If the number is > 50, it's almost certainly a price or code, not a person count
                            if v > 50:
                                # Treat as add-on, not participant
                                is_participant = False
                            else:
                                if 'total:' in t_lower:
                                    is_participant = False # Ignore total lines
                                elif 'adult' in t_lower: res["adt"] += v
                                elif 'student' in t_lower: res["std"] += v
                                elif 'children' in t_lower or 'child' in t_lower: res["chd"] += v
                                elif 'infant' in t_lower: res["inf"] += v
                                elif 'youth' in t_lower: res["youth"] += v
                                # Support generic "people" -> Adult
                                elif ('people' in t_lower or 'person' in t_lower): res["adt"] += v
                                else:
                                    is_participant = False # Number found but no keyword match
                        else:
                            is_participant = False
                        
                        # Identify Add-Ons
                        # Logic: If it's not a standard participant line (Adult/Student/Child/Infant/Youth with count), treat as add-on
                        # Example Add-on: "TutAnghAmoon Tomb entry fee: Adult - €100.00"
                        # Standard line usually starts with number then type.
                        
                        if nm and v <= 50 and 'total:' not in t_lower and any(k in t_lower for k in ['adult', 'student', 'child', 'infant', 'youth']):
                             is_participant = True
                        
                        if not is_participant and t and 'total:' not in t_lower:
                             # Clean up price if needed, or keep full text
                             # e.g. "TutAnghAmoon Tomb entry fee: Adult - €100.00"
                             extras.append(t)
                             
                    if extras:
                        res["add_ons"] = "; ".join(extras)
            except Exception:
                pass
            
            if res["email"] or res["phone"]:
                return res
                
        except Exception as e:
            self.logger.error(f"Failed to fetch details from subpage {url}: {e}")
        finally:
            if new_page:
                try:
                    await new_page.close()
                except Exception:
                    pass
        return res

    async def extract_bookings_from_page(self) -> List[Dict]:
        items: List[Dict] = []
        seen = set()
        try:
            await self._handle_overlays()
            await self.page.wait_for_selector(self.SELECTORS["booking_card"], timeout=30000)
        except Exception:
            try:
                await self.page.wait_for_selector('[data-testid*="booking"], [data-test*="booking"], [data-test-id*="booking"], div[class*="Card"], section[class*="booking"], article[class*="Card"]', timeout=20000)
            except Exception:
                return items
        try:
            await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1.2)
        except Exception:
            pass
        selectors = '[data-testid="booking-card"]'
        # Fallback to broader selectors ONLY if the specific one yields nothing
        cards = await self.page.query_selector_all(selectors)
        if not cards:
            selectors = '[data-testid*="booking"], [data-test*="booking"], [data-test-id*="booking"], div[class*="Card"], section[class*="booking"], article[class*="Card"]'
            cards = await self.page.query_selector_all(selectors)
        
        try:
            for fr in self.page.frames:
                # Use only specific selector inside frames first
                found = await fr.query_selector_all('[data-testid="booking-card"]')
                if found:
                    cards.extend(found)
        except Exception:
            pass
            
        # Filter out hidden or tiny elements that are likely not real cards
        visible_cards = []
        for card in cards:
            try:
                # Check visibility
                is_visible = await card.is_visible()
                if not is_visible:
                    continue
                    
                # Check bounding box size to filter out icons/small fragments matched by broad selectors
                box = await card.bounding_box()
                if box and (box['width'] < 50 or box['height'] < 50):
                    continue
                    
                visible_cards.append(card)
            except Exception:
                pass
        cards = visible_cards

        try:
            self.logger.info(f"Found {len(cards)} booking cards on page")
        except Exception:
            pass
        for idx in range(len(cards)):
            card = cards[idx]
            try:
                btn = await card.query_selector(self.SELECTORS["show_details_btn"]) 
                if btn:
                    await btn.click()
                    await asyncio.sleep(1.5)
            except Exception:
                pass
            text = await card.text_content()
            if not text:
                continue
            # Debug: log text content to see if booking numbers are present
            self.logger.info(f"Card text content (first 200 chars): {text[:200]}")
            # Prefer explicit booking reference element; fallback to regex
            booking_nr = None
            try:
                br = await card.query_selector(self.SELECTORS["booking_reference"]) 
                if br:
                    ref_txt = (await br.text_content() or '').strip()
                    mm = re.search(r'(GYG[A-Z0-9]+)', ref_txt)
                    booking_nr = _sanitize_booking_nr(mm.group(1)) if mm else None
            except Exception:
                pass
            if not booking_nr:
                m = re.search(r'(GYG[A-Z0-9]+)', text)
                booking_nr = _sanitize_booking_nr(m.group(1)) if m else None
            self.logger.info(f"Booking number extraction (sanitized): {booking_nr} (raw_match: {m.group(1) if m else None})")
            if not booking_nr or booking_nr in seen:
                continue
            seen.add(booking_nr)
            product_id = None
            try:
                link = await card.query_selector(self.SELECTORS["activity_title"]) 
                href = await link.get_attribute('href') if link else None
                if href:
                    mm = re.search(r'tour_id=(\d+)', href)
                    product_id = mm.group(1) if mm else None
            except Exception:
                pass
            trip_name = None
            try:
                tn = await card.query_selector(self.SELECTORS["activity_title"]) 
                trip_name = (await tn.text_content() or '').strip() if tn else None
            except Exception:
                pass
            option_selected = None
            try:
                opt = await card.query_selector(self.SELECTORS["option_text"]) 
                option_selected = ((await opt.text_content()) or '').replace('Option: ', '').strip() if opt else None
            except Exception:
                pass
            date_trip = None
            try:
                dt = await card.query_selector(self.SELECTORS["activity_date"]) 
                raw_dt = (await dt.text_content() or '').strip() if dt else None
                date_trip = _parse_date_text(raw_dt) or raw_dt
            except Exception:
                pass
            total_price_eur = None
            try:
                price_el = await card.query_selector(self.SELECTORS["participants"]) 
                price_text = await price_el.text_content() if price_el else ''
                pm = re.search(r'€\s*([\d.,]+)', price_text or '')
                total_price_eur = float(pm.group(1).replace(',', '.')) if pm else None
            except Exception:
                pass
            adt = 0; std = 0; chd = 0; inf = 0; youth = 0
            add_ons = None
            try:
                items_el = await card.query_selector_all('[data-testid="participants-breakdown-items"] li')
                extras = []
                for it in items_el or []:
                    t = (await it.text_content() or '').strip()
                    tl = t.lower()
                    
                    # Check for participants
                    # Improved logic: Try start of line first (Standard GYG format: "5 Adults")
                    nm = re.search(r'^(\d+)', t)
                    if not nm:
                         # Fallback: Try to find any number (Old method)
                         nm = re.search(r'(\d+)', t)

                    v = int(nm.group(1)) if nm else 0
                    
                    # Safety: Ignore numbers > 50 (likely prices, ages, or percentages)
                    # Also ignore if it's the price of an add-on (e.g. €20.00)
                    if v > 50 or (v >= 10 and not re.search(r'^\s*\d+', t)): 
                        v = 0
                    
                    is_participant = False
                    
                    # Only treat as participant if we have a valid count (>0)
                    # This ensures that clamped values (from prices >50) or lines without numbers
                    # are NOT treated as participants, and thus will be added to extras (Add-Ons).
                    if v > 0:
                        if 'total:' in tl:
                             is_participant = False # Ignore total lines
                        elif 'adult' in tl: 
                            # If we already set adults from another line, don't overwrite it unless we're combining
                            if adt == 0: adt = v
                            is_participant = True
                        elif 'student' in tl: std = v; is_participant = True
                        elif 'children' in tl or 'child' in tl: chd = v; is_participant = True
                        elif 'infant' in tl: inf = v; is_participant = True
                        elif 'youth' in tl: youth = v; is_participant = True
                        
                        # Fix for "Total: 4 people" confusion
                        elif ('people' in tl or 'person' in tl):
                             pass # Handled below
                        
                        if not is_participant and ('people' in tl or 'person' in tl) and 'total:' not in tl:
                             if adt == 0: adt = v
                             is_participant = True
                             
                        # Prevent extracting price (e.g. €20.00) as participant count of 20
                        # Usually participant counts are at the START of the string, e.g. "2 Adults"
                        # If the regex found the number but it's not at the very beginning, and it's a large number, reject it
                        if is_participant and not re.search(r'^\s*\d+', t) and v >= 10:
                             is_participant = False
                             if 'adult' in tl: adt = 0
                             elif 'student' in tl: std = 0
                             elif 'children' in tl or 'child' in tl: chd = 0
                             elif 'infant' in tl: inf = 0
                             elif 'youth' in tl: youth = 0
                             elif ('people' in tl or 'person' in tl) and 'total:' not in tl: adt = 0
                    
                    # If not a standard participant, treat as add-on
                    if not is_participant and t and 'total:' not in tl:
                        extras.append(t)
                        
                if extras:
                    add_ons = "; ".join(extras)
            except Exception:
                pass
            
            customer_name = None; customer_country = None; customer_phone = None; customer_email = None
            try:
                customer_name, customer_country, customer_phone, customer_email = await self.extract_customer_info(card)
                
                # Fallback: If email is missing OR we want to force AI enhancement for details/add-ons
                should_fetch_details = not customer_email
                # If AI is enabled and we suspect missing add-ons or just want full details, we can force it.
                # However, for performance, let's only do it if email is missing OR if we have AI key and no add_ons found yet.
                if self.deepseek_key and not add_ons:
                    should_fetch_details = True
                
                if should_fetch_details:
                    # Prefer using the dedicated message link if available, as it is cleaner
                    msg_btn = await card.query_selector('[data-testid="message-customer"]')
                    if msg_btn:
                        href = await msg_btn.get_attribute('href')
                        if href:
                            self.logger.info(f"Fetching details for {booking_nr} from subpage (Email missing or AI scan requested)...")
                            # The user suggests opening in a new tab for better processing
                            # fetch_details_from_subpage already opens a new context/page, so it effectively does this.
                            # We just need to ensure we use the correct href.
                            details = await self.fetch_details_from_subpage(href)
                            if details.get("email") and not customer_email:
                                customer_email = details.get("email")
                                self.logger.info(f"Found email on subpage: {customer_email}")
                            if details.get("phone") and not customer_phone:
                                customer_phone = details.get("phone")
                                self.logger.info(f"Found phone on subpage: {customer_phone}")
                            
                            # Update participant counts if found on subpage
                            # PROTECTIVE LOGIC: Only overwrite if subpage returned actual counts (sum > 0)
                            # This prevents overwriting valid card data with zeros if subpage fetch failed (e.g. login page)
                            sub_sum = (details.get("adt") or 0) + (details.get("std") or 0) + (details.get("chd") or 0) + (details.get("inf") or 0) + (details.get("youth") or 0)
                            
                            if sub_sum > 0:
                                adt = details.get("adt"); std = details.get("std"); chd = details.get("chd"); inf = details.get("inf"); youth = details.get("youth")
                                self.logger.info(f"Updated participants from subpage: A:{adt} S:{std} C:{chd} I:{inf} Y:{youth}")
                            else:
                                self.logger.warning(f"Subpage returned 0 participants (possible login/error page). Keeping card data: A:{adt} S:{std} C:{chd} I:{inf} Y:{youth}")
                            
                            # Update Add-Ons if found on subpage
                            if details.get("add_ons"):
                                add_ons = details.get("add_ons")
                                self.logger.info(f"Found Add-Ons on subpage: {add_ons}")
            except Exception as e:
                self.logger.warning(f"Failed to extract customer info or fetch details for {booking_nr}: {e}")
            google_maps = None
            try:
                gm = await card.query_selector(self.SELECTORS["google_maps"]) 
                google_maps = await gm.get_attribute('href') if gm else None
            except Exception:
                pass
            hotel_name = None
            try:
                # 1. Try explicit data-testid for accommodation (Most accurate)
                acc_el = await card.query_selector('[data-testid="customer-accommodation"]')
                if acc_el:
                    hotel_text = await acc_el.text_content()
                else:
                    # 2. Fallback: Prefer explicit Location line under Pickup details
                    loc_label = await card.query_selector('text=Location')
                    if loc_label:
                        # Try the next sibling text content
                        try:
                            hotel_text = await loc_label.evaluate("(el)=>{const n=el.nextElementSibling;return n?n.textContent:''}")
                        except Exception:
                            hotel_text = None
                    else:
                        hotel_text = None

                if hotel_text:
                    txt = hotel_text.strip()
                    if 'Edit location details' in txt:
                        txt = txt.split('Edit location details')[0].strip()
                    
                    # Intelligent extraction: Skip postal codes or numeric prefixes
                    # e.g. "84521, Hurghada..." -> "Hurghada"
                    # e.g. "Hilton Hotel, Hurghada..." -> "Hilton Hotel"
                    parts = [p.strip() for p in txt.split(',')]
                    
                    selected_part = None
                    for p in parts:
                        # Skip if empty
                        if not p: continue
                        
                        # Skip if purely numeric (e.g. "84521")
                        if p.isdigit(): continue
                        
                        # Skip if it looks like a short postal code (mixed letters/numbers but short, e.g. "G1 5HB" or "84521")
                        # Heuristic: If < 7 chars and contains digits, might be a code. 
                        # But "W Hotel" is short. "101 Hotel" has digits.
                        # Let's stick to: Skip if purely numeric OR matches specific postal code patterns if needed.
                        # For now, purely numeric check solves the user's "84521" case.
                        
                        selected_part = p
                        break
                    
                    if selected_part:
                        hotel_name = selected_part
                    else:
                        # If all parts were numeric (unlikely) or empty, use the first non-empty part
                        hotel_name = parts[0] if parts else txt

                if not hotel_name:
                    # Fallback: parse from full card text by regex near 'Pickup details' section
                    mloc = re.search(r'Pickup details[\s\S]*?Location\s*([^\n]+)', text or '', re.IGNORECASE)
                    if mloc:
                        txt = mloc.group(1).strip()
                        if 'Edit location details' in txt:
                            txt = txt.split('Edit location details')[0].strip()
                        # Apply same logic
                        parts = [p.strip() for p in txt.split(',')]
                        for p in parts:
                            if p and not p.isdigit():
                                hotel_name = p
                                break
                        if not hotel_name and parts:
                             hotel_name = parts[0]
            except Exception:
                pass
            guide = None
            try:
                gl = await card.query_selector(self.SELECTORS["guide_language"] + ' > .text-body') 
                guide = ((await gl.text_content()) or '').replace('Live guide: ', '').strip() if gl else None
            except Exception:
                pass
            pickup_time = None
            try:
                pt = await card.query_selector('div.mt-4 > p.font-medium')
                ptxt = (await pt.text_content() or '').strip() if pt else None
                if ptxt and ptxt.lower().startswith('pickup at'):
                    pickup_time = ptxt.replace('Pickup at', '').strip()
            except Exception:
                pass
            retail_price = None; revenue = None; commission_breakdown = None; supplier_commission = None; extra_commission = None
            try:
                retail_price, revenue, commission_breakdown, supplier_commission, extra_commission = await self.extract_financial_breakdown(card, booking_nr)
            except Exception as e:
                self.logger.warning(f"Failed to extract financial breakdown for {booking_nr}: {e}")
            status = 'Confirmed'
            try:
                # Improved status extraction: Check the tag container first
                st_tag = await card.query_selector('[data-testid="booking-status-tag"]')
                if st_tag:
                    # Try to find the specific label inside
                    st_label = await st_tag.query_selector('.p-tag-label')
                    if st_label:
                        sval = (await st_label.text_content() or '').strip().lower()
                    else:
                        # Fallback: Read text from the main tag
                        sval = (await st_tag.text_content() or '').strip().lower()
                    
                    if 'canceled' in sval or 'cancelled' in sval:
                        # Guard against "free cancellation" if it somehow appears in the tag
                        if 'free' not in sval:
                            status = 'Canceled'
                    elif 'changed' in sval:
                        status = 'Changed'
                    elif 'pending' in sval:
                        status = 'Pending'
                    elif 'rejected' in sval:
                        status = 'Rejected'
                    elif 'confirmed' in sval:
                         status = 'Confirmed'
            except Exception:
                pass
            destination = extract_region(trip_name, option_selected, text) or 'Unknown'
            # add_ons extracted earlier
            
            ticket_codes = []
            try:
                ticket_codes = await self.extract_ticket_codes(card)
            except Exception:
                pass
            booking = {
                "id": f"booking_{booking_nr}",
                "booking_nr": booking_nr,
                "agency": "GetYourGuide",
                "customer_name": customer_name,
                "customer_country": customer_country,
                "customer_phone": customer_phone,
                "customer_email": customer_email,
                "trip_name": trip_name or destination,
                "product_id": product_id,
                "destination": destination,
                "option_selected": option_selected,
                "date_trip": date_trip,
                "total_price_eur": retail_price if retail_price is not None else total_price_eur,
                "retail_price": retail_price,
                "revenue": revenue,
                "commission_breakdown": commission_breakdown,
                "supplier_commission": supplier_commission,
                "extra_commission": extra_commission,
                "google_maps": google_maps,
                "hotel_name": hotel_name,
                "guide": guide,
                "pickup_time": pickup_time,
                "traveler_name": None,
                "add_ons": add_ons,
                "adt": adt,
                "std": std,
                "chd": chd,
                "inf": inf,
                "youth": youth,
                "booking_status": status,
                "ticket_codes": ticket_codes,
                "raw_data": {"text": (text or "")[:200]},
            }
            booking = self.validate_booking_data(booking)
            try:
                self.logger.info(
                    f"Extracted {booking_nr} | trip='{booking['trip_name']}' | price={booking['total_price_eur']} | participants A:{adt} S:{std} C:{chd} I:{inf} Y:{youth} | status={status} | retail={retail_price} revenue={revenue} comm={commission_breakdown}"
                )
            except Exception:
                pass
            if self.deepseek_key:
                try:
                    # Pass empty string for breakdown text as we don't capture it explicitly
                    improved = ai_enhance(self.deepseek_key, text or '', '', booking)
                    for k, v in (improved or {}).items():
                        # Prioritize AI values for participants and price if they are valid (>0)
                        # This helps correct regex errors (e.g. reading "2026" as 20 adults)
                        if k in ('adt', 'chd', 'inf', 'youth', 'std', 'total_price_eur') and isinstance(v, (int, float)) and v > 0:
                             booking[k] = v
                        elif v is not None and (booking.get(k) in (None, '', 0)):
                            booking[k] = v
                    rd = booking.get("raw_data") or {}
                    rd["ai"] = improved or {}
                    booking["raw_data"] = rd
                    self.logger.info(f"AI enhanced {booking_nr} with keys: {list((improved or {}).keys())}")
                except Exception as e:
                    self.logger.warning(f"AI enhancement block failed for {booking_nr}: {e}")
            items.append(booking)
        return items

    async def extract_financial_breakdown(self, card, booking_nr: Optional[str] = None) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
        """Extract totals (retail, revenue, commission) from breakdown table footer row.
        Steps:
        1. Ensure breakdown is visible (click button if present)
        2. Wait for table footer total row
        3. Parse cells 2,3,4 for amounts and percentage
        4. Parse commission details for supplier/extra rates
        """
        try:
            br = await card.query_selector(self.SELECTORS["show_breakdown_btn"]) 
            if br:
                await br.click()
                await asyncio.sleep(0.4)
            await card.wait_for_selector(self.SELECTORS["breakdown_total_row"], timeout=5000)
        except Exception:
            pass
        retail = None; revenue = None; commission = None; supplier_rate = None; extra_rate = None
        try:
            total_row = await card.query_selector(self.SELECTORS["breakdown_total_row"]) 
            if not total_row:
                tbl = await card.query_selector(self.SELECTORS["breakdown_table"]) 
                if tbl:
                    rows = await tbl.query_selector_all('tfoot tr')
                    total_row = rows[-1] if rows else None
            if total_row:
                tds = await total_row.query_selector_all('td')
                if len(tds) >= 4:
                    rtxt = (await tds[1].text_content() or '').strip()
                    vtxt = (await tds[2].text_content() or '').strip()
                    ctxt = (await tds[3].text_content() or '').strip()
                    retail = self.parse_euro_amount(rtxt)
                    revenue = self.parse_euro_amount(vtxt)
                    commission = self.parse_commission_rate(ctxt)
                    details = self.parse_commission_details(ctxt)
                    supplier_rate = details.get('supplier_rate')
                    extra_rate = details.get('extra_rate')
        except Exception as e:
            self.logger.error(
                f"Financial extraction failed", extra={
                    'booking_nr': booking_nr,
                    'error': str(e),
                    'extraction_phase': 'breakdown_table'
                }
            )
        # Fallback compute commission from retail/revenue
        if commission is None and retail is not None and revenue is not None and retail > 0:
            commission = round(((retail - revenue) / retail) * 100, 2)
        return retail, revenue, commission, supplier_rate, extra_rate

    def parse_euro_amount(self, text: str) -> Optional[float]:
        """Convert euro amount text (e.g., '€1,234.56') to float"""
        if not text:
            return None
        m = re.search(r'€\s*([\d.,]+)', text)
        if not m:
            return None
        val = m.group(1).strip()
        try:
            # Normalize thousands separators; assume '.' decimal per observed page
            val = val.replace(',', '')
            return float(val)
        except Exception:
            try:
                val = val.replace('.', '').replace(',', '.')
                return float(val)
            except Exception:
                return None

    def parse_commission_rate(self, text: str) -> Optional[float]:
        """Extract first percentage as float from text like '32.00%'."""
        if not text:
            return None
        m = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*%', text)
        if not m:
            return None
        val = float(m.group(1))
        # Keep as percentage value (e.g. 32.0), it will be divided by 100 before sending to Airtable
        return val

    def parse_commission_details(self, text: str) -> Dict[str, Optional[float]]:
        """Extract commission details: total, supplier_rate, extra_rate."""
        total = self.parse_commission_rate(text) or None
        supplier = None
        extra = None
        m1 = re.search(r'Supplier\s+commission\s+rate\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*%', text, re.IGNORECASE)
        if m1:
            supplier = float(m1.group(1))
        m2 = re.search(r'Extra\s+commission[^:]*:\s*([0-9]+(?:\.[0-9]+)?)\s*%', text, re.IGNORECASE)
        if m2:
            extra = float(m2.group(1))
        return {"total": total, "supplier_rate": supplier, "extra_rate": extra}

    async def extract_customer_info(self, card) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Extract name and country from lead traveler element as 'Name (Country)'."""
        name = None; country = None; phone = None; email = None
        ln = await card.query_selector(self.SELECTORS["lead_traveler"]) 
        if ln:
            txt = (await ln.text_content() or '').strip()
            m = re.match(r'^(.+?)\s*\(([^)]+)\)', txt)
            if m:
                name = m.group(1).strip()
                country = m.group(2).strip()
            else:
                name = txt.split('(')[0].strip()
        ph = await card.query_selector(self.SELECTORS["lead_phone"]) 
        phone = ((await ph.text_content()) or '').strip() if ph else None
        
        # Email extraction
        try:
            # 1. Try mailto link first (most reliable if present)
            mailto = await card.query_selector('a[href^="mailto:"]')
            if mailto:
                href = await mailto.get_attribute('href')
                if href:
                    email = href.replace('mailto:', '').strip()
            
            # 2. If not found, look for proxy email pattern in card text
            if not email:
                card_text = await card.text_content()
                # Matches standard emails and GYG proxy emails (e.g. customer-xyz@reply.getyourguide.com)
                emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', card_text)
                
                # Filter and prioritize emails starting with 'customer'
                customer_emails = [em for em in emails if em.lower().startswith('customer')]
                
                if customer_emails:
                    email = customer_emails[0]
                elif emails:
                    # Fallback only if no 'customer' email is found, but filter internal ones
                    for em in emails:
                        if "getyourguide.com" in em.lower() and "reply" not in em.lower() and "customer" not in em.lower():
                             continue
                        email = em
                        break
        except Exception:
            pass
            
        return name, country, phone, email

    async def extract_ticket_codes(self, card) -> List[str]:
        """Extract ticket codes from tickets section or card text."""
        codes: List[str] = []
        try:
            tickets_section = await card.query_selector('section:has-text("Tickets"), div:has-text("Tickets")')
            ttxt = (await tickets_section.text_content()) if tickets_section else await card.text_content()
            for m in re.findall(r'\b[A-Z0-9]{8,}\b', (ttxt or '')):
                if m.startswith('GYG'):
                    continue
                codes.append(m)
        except Exception:
            pass
        return list(dict.fromkeys(codes))

    def validate_booking_data(self, booking: Dict) -> Dict:
        """Validate completeness/consistency and attach warnings list."""
        warnings: List[str] = []
        required = ['booking_nr', 'trip_name', 'date_trip']
        for k in required:
            if not booking.get(k):
                warnings.append(f'missing_{k}')
        rp = booking.get('retail_price')
        rv = booking.get('revenue')
        cm = booking.get('commission_breakdown')
        try:
            if isinstance(rp, (int, float)) and isinstance(rv, (int, float)) and rp > 0 and isinstance(cm, (int, float)):
                calc = round(rp * (1 - cm/100), 2)
                if rv is not None and abs(calc - float(rv)) > 1.0:
                    warnings.append('financial_mismatch')
        except Exception:
            warnings.append('financial_validation_error')
        booking['validation_warnings'] = warnings
        return booking

    async def _save_session_state(self):
        """Save browser state to file to persist login across restarts."""
        if self.context and not self.persistent:
            try:
                await self.context.storage_state(path=self.storage_state_path)
                # self.logger.debug("Session state saved.")
            except Exception:
                pass

    async def sync_booking(self, booking: Dict) -> bool:
        self.logger.info(f"Sync start: {booking.get('booking_nr')} -> DB + Airtable")
        # Save session state periodically during sync activity
        await self._save_session_state()
        
        db_res = self.db.save_booking(booking)
        if not db_res.get("success"):
            self.logger.error(f"DB save failed for {booking.get('booking_nr')}: {db_res.get('error')}")
            return False
            
        status = db_res.get("status", "updated")
        is_synced = bool(db_res.get("synced_to_airtable"))
        previous_record = db_res.get("previous_record") or {}
        
        # --- LOGIC UPDATE: Compare Platform vs Internal Memory (DB) ---
        # Only sync to Airtable if:
        # 1. The booking is NEW (status == 'created')
        # 2. The booking has CHANGED compared to DB (status == 'updated')
        # 3. The booking was never synced before (is_synced == False)
        
        if status == "unchanged" and is_synced:
             # Case: Data matches DB and already synced -> DO NOT SYNC to preserve Airtable manual edits
             self.logger.info(f"Skipping Airtable sync for {booking.get('booking_nr')} (No changes from platform & already synced)")
             return True
        
        force_update_fields = []
        if previous_record:
            old_date = str(previous_record.get("date_trip")) if previous_record.get("date_trip") else None
            new_date = str(booking.get("date_trip")) if booking.get("date_trip") else None
            # Only force update if the actual date has changed in DB
            if old_date and new_date and old_date != new_date:
                force_update_fields.append("Date Trip")
                # When Date Trip changes, also force update Real Date Trip
                force_update_fields.append("Real Date Trip")

            old_trip_name = previous_record.get("trip_name")
            new_trip_name = booking.get("trip_name")
            if old_trip_name and new_trip_name and old_trip_name != new_trip_name:
                force_update_fields.append("trip Name")

        at_res = self.airtable.upsert_booking(booking, force_update_fields=force_update_fields)
        if at_res.get("success"):
            self.db.mark_synced(booking.get("booking_nr"), at_res.get("record_id"))
            self.logger.info(f"Airtable synced {booking.get('booking_nr')} record_id={at_res.get('record_id')}")
            return True
        else:
            code = at_res.get('code') or at_res.get('error')
            self.logger.error(f"Airtable sync failed for {booking.get('booking_nr')} code={code}")
        return False

    async def _prepare_extraction_page(self, target_url: str) -> bool:
        should_navigate = True
        try:
            if "bookings" in self.page.url:
                 should_navigate = False
                 self.logger.info("Already on bookings page. Skipping initial navigation.")
        except Exception:
             pass

        if should_navigate:
            await self.page.goto(target_url, wait_until="domcontentloaded")
            try:
                await self.page.evaluate("document.body.style.zoom = '65%'")
            except Exception:
                pass
            
        try:
            await self.page.wait_for_load_state('domcontentloaded', timeout=8000)
            await asyncio.sleep(2)
        except Exception:
            await asyncio.sleep(3)
        try:
            await self._handle_overlays()
        except Exception:
            pass
        try:
            await self.suppress_chat_widgets()
        except Exception:
            pass
        return True

    async def run_extraction(self):
        target_url = "https://supplier.getyourguide.com/bookings"
        if self.managed_by:
            target_url += f"?managed_by={self.managed_by}"
            
        await self._prepare_extraction_page(target_url)
        
        total_synced = 0
        total_failed = 0
        page_num = max(1, int(self.current_page or 1))
        try:
            await self._navigate_to_page(page_num)
        except Exception:
            pass
        
        while True:
            ok = await self.check_session()
            if not ok:
                await asyncio.sleep(2)
                continue
            
            self.logger.info(f"Extracting from page {page_num}")
            try:
                await self.page.evaluate("document.body.style.zoom = '65%'")
            except Exception:
                pass
            bookings = await self.extract_bookings_from_page()
            
            if not bookings:
                 self.logger.warning(f"No bookings found on page {page_num} after click. Trying direct URL navigation.")
                 url = "https://supplier.getyourguide.com/bookings"
                 if self.managed_by:
                     url += f"?managed_by={self.managed_by}"
                     url += f"&page_default={max(0, page_num-1)}"
                 else:
                     url += f"?page_default={max(0, page_num-1)}"
                 
                 try:
                     await self.page.goto(url, wait_until="domcontentloaded")
                     await asyncio.sleep(5)
                     await self.page.evaluate("document.body.style.zoom = '65%'")
                     bookings = await self.extract_bookings_from_page()
                 except Exception:
                     pass

            if not bookings:
                try:
                    self.logger.warning(f"No bookings found on page {page_num}. Reloading page to verify...")
                    await self.page.reload(wait_until="domcontentloaded")
                    await asyncio.sleep(3)
                    bookings = await self.extract_bookings_from_page()
                except Exception:
                    pass
            
            if not bookings:
                if not await self.check_session():
                     continue
                     
                self.logger.warning(f"No bookings found on page {page_num} after reload. Assuming end of list or transient error.")
                await self._navigate_to_first_page()
                page_num = 1
                self.current_page = 1
                continue
                
            for b in bookings:
                if await self.sync_booking(b):
                    total_synced += 1
                else:
                    total_failed += 1
            
            if page_num < self.max_pages and await self._navigate_next_page():
                page_num += 1
                self.current_page = page_num
            else:
                if page_num >= self.max_pages:
                    self.logger.info(f"Reached maximum page limit ({self.max_pages}) — wrapping to first page")
                else:
                    self.logger.info("Reached end of pages via navigation — wrapping to first page")
                
                if getattr(self, "run_once_flag", False):
                    self.logger.info("Run once flag is set, exiting extraction loop.")
                    break
                
                if self.restart_delay_minutes > 0:
                    self.logger.info(f"Cycle complete. Waiting {self.restart_delay_minutes} minutes before restarting...")
                    await asyncio.sleep(self.restart_delay_minutes * 60)
                await self._navigate_to_first_page()
                page_num = 1
                self.current_page = 1
                
        return {"synced": total_synced, "failed": total_failed}
    
    async def _get_total_pages(self) -> int:
        """Get the total number of pages from pagination"""
        try:
            # Look for the last page number in the pagination
            page_buttons = await self.page.query_selector_all('button[aria-label*="Page"], button.p-paginator-page')
            page_numbers = []
            
            for btn in page_buttons:
                text = await btn.text_content()
                if text and text.strip().isdigit():
                    page_numbers.append(int(text.strip()))
            
            if page_numbers:
                return max(page_numbers)
                
            # Alternative: look for "Page X of Y" text
            page_info = await self.page.query_selector('text=/Page\\s+\\d+\\s+of\\s+\\d+/')
            if page_info:
                text = await page_info.text_content()
                match = re.search(r'Page\\s+\\d+\\s+of\\s+(\\d+)', text)
                if match:
                    return int(match.group(1))
                    
            # Alternative: look for total count and calculate pages
            total_text = await self.page.query_selector('text=/Total.*\\d+.*bookings/')
            if total_text:
                text = await total_text.text_content()
                match = re.search(r'Total.*?(\\d+).*bookings', text)
                if match:
                    total_bookings = int(match.group(1))
                    # Assume 10 bookings per page (common default)
                    return (total_bookings + 9) // 10
                    
        except Exception as e:
            self.logger.warning(f"Error getting total pages: {e}")
            
        return 1  # Default to 1 page if we can't determine
    
    async def _navigate_to_first_page(self) -> bool:
        # Wrap button click in try/except to fallback to URL
        try:
            first_btn = await self.page.query_selector('button:has-text("First Page")')
            if not first_btn:
                first_btn = await self.page.query_selector('button:has-text("First")')
            if first_btn:
                await first_btn.click()
                try:
                    await self.page.wait_for_load_state('domcontentloaded', timeout=5000)
                    await asyncio.sleep(2)
                except Exception:
                    pass
                return True
        except Exception:
             pass

        # URL Fallback
        try:
            url = "https://supplier.getyourguide.com/bookings"
            if self.managed_by:
                url += f"?managed_by={self.managed_by}&page_default=0"
            else:
                url += "?page_default=0"
            
            await self.page.goto(url, wait_until="load")
            try:
                await self.page.wait_for_load_state('domcontentloaded', timeout=5000)
                await asyncio.sleep(2)
            except Exception:
                pass
            return True
        except Exception as e:
            try:
                self.logger.error(f"Navigation to first page failed: {e}")
            except Exception:
                pass
            await self._restart_from_beginning("navigate_first_failed")
            return False

    async def _navigate_next_page(self) -> bool:
        # 1. Try clicking Next button
        try:
            next_btn = await self.page.query_selector('button:has-text("Next Page")')
            if not next_btn:
                next_btn = await self.page.query_selector('button:has-text("Next")')
            if next_btn:
                await next_btn.click()
                try:
                    await self.page.wait_for_load_state('domcontentloaded', timeout=5000)
                    await asyncio.sleep(2)
                except Exception:
                    pass
                return True
        except Exception:
            pass

        # 2. Identify current page and try _navigate_to_page(next)
        next_page_num = None
        try:
            active_btn = await self.page.query_selector('button[aria-current="page"], button.p-paginator-page.p-paginator-page-selected')
            if active_btn:
                t = await active_btn.text_content()
                if t and t.strip().isdigit():
                    next_page_num = int(t.strip()) + 1
        except Exception:
            pass
            
        if next_page_num is None and self.current_page:
            next_page_num = self.current_page + 1
            
        if next_page_num:
            try:
                 self.logger.info(f"Next button failed, attempting fallback to page {next_page_num}")
            except Exception:
                 pass
            return await self._navigate_to_page(next_page_num)
            
        return False
    async def _navigate_to_page(self, page_number: int) -> bool:
        # 1. Try specific page button
        try:
            page_button = await self.page.query_selector(f'button[aria-label="Page {page_number}"]')
            if not page_button:
                page_button = await self.page.query_selector(f'button.p-paginator-page:has-text("{page_number}")')
            if page_button:
                is_selected = await page_button.get_attribute('aria-current') == 'page' or \
                             'p-paginator-page-selected' in (await page_button.get_attribute('class') or '')
                if is_selected:
                    try:
                        self.logger.info(f"Already on page {page_number}")
                    except:
                        pass
                    return True
                await page_button.click()
                try:
                    self.logger.info(f"Clicked page {page_number} button")
                    await self.page.wait_for_load_state('domcontentloaded', timeout=5000)
                    await asyncio.sleep(2)
                except Exception:
                    await asyncio.sleep(1)
                return True
        except Exception:
             pass

        # 2. URL Fallback
        try:
            url = "https://supplier.getyourguide.com/bookings"
            if self.managed_by:
                url += f"?managed_by={self.managed_by}"
                url += f"&page_default={max(0, page_number-1)}"
            else:
                url += f"?page_default={max(0, page_number-1)}"
            
            try:
                 self.logger.info(f"Navigating via URL to page {page_number}: {url}")
            except:
                 pass

            await self.page.goto(url, wait_until="load")
            try:
                await self.page.wait_for_load_state('domcontentloaded', timeout=5000)
                await asyncio.sleep(2)
            except Exception:
                await asyncio.sleep(1)
            
            # Verify we are on correct page
            active_btn = await self.page.query_selector('button[aria-current="page"], button.p-paginator-page.p-paginator-page-selected')
            t = await active_btn.text_content() if active_btn else None
            if t and t.strip().isdigit() and int(t.strip()) == page_number:
                return True
            
            # Even if verification fails, we return True because we did the "last attempt" navigation
            return True
        except Exception as e:
            try:
                self.logger.error(f"Direct navigation to page {page_number} failed: {e}")
            except Exception:
                pass
            
            # If even direct URL failed, then we might need to restart
            await self._restart_from_beginning("navigate_page_failed")
            return False

    async def run_once(self) -> bool:
        if not await self.initialize_browser():
            return False
        if not await self.login():
            await self._safe_close()
            return False
        try:
            await self.run_extraction()
            return True
        finally:
            await self._safe_close()

    async def run_server(self):
        if not self.auto_sync:
            return
        try:
            if not await self.initialize_browser():
                return
            if not await self.login():
                pass
            self.logger.info("Persistent server started; browser will remain open")
            while True:
                try:
                    if not await self._ensure_page_responsive():
                        await self._recover_from_failure("pre_extraction_unresponsive")
                        await asyncio.sleep(1)
                        continue
                    await self.run_extraction()
                except Exception as e:
                    try:
                        await self._recover_from_failure(str(e))
                    except Exception:
                        try:
                            await self.login()
                        except Exception:
                            pass
                try:
                    await asyncio.sleep(self.sync_interval * 60)
                except KeyboardInterrupt:
                    break
        finally:
            # Keep browser running unless interrupted
            await self._safe_close()

    async def _safe_close(self):
        try:
            if self.context and not self.persistent:
                try:
                    await self.context.storage_state(path=self.storage_state_path)
                except Exception:
                    pass
                try:
                    await self.context.close()
                except Exception:
                    pass
            elif self.context and self.persistent:
                pass
            elif self.browser:
                await self.browser.close()
        except Exception:
            pass
        try:
            if self.playwright:
                await self.playwright.stop()
        except Exception:
            pass

    async def _clear_browser_data(self):
        try:
            if self.page:
                try:
                    await self.page.evaluate("() => { try{localStorage.clear()}catch(e){}; try{sessionStorage.clear()}catch(e){} }")
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if self.context:
                try:
                    await self.context.clear_cookies()
                except Exception:
                    pass
                try:
                    await self.context.close()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if self.browser:
                try:
                    await self.browser.close()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if self.playwright:
                try:
                    await self.playwright.stop()
                except Exception:
                    pass
        except Exception:
            pass

        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

        try:
            if os.path.exists(self.storage_state_path):
                os.remove(self.storage_state_path)
        except Exception:
            pass
        try:
            if self.user_data_dir and os.path.exists(self.user_data_dir):
                shutil.rmtree(self.user_data_dir, ignore_errors=True)
        except Exception:
            pass

    async def _ensure_page_responsive(self) -> bool:
        try:
            if not self.page:
                return False
            try:
                val = await asyncio.wait_for(self.page.evaluate("() => document.readyState"), timeout=5)
            except Exception:
                val = None
            if val not in ("loading", "interactive", "complete"):
                return False
            try:
                await asyncio.wait_for(self.page.evaluate("() => Date.now()"), timeout=5)
            except Exception:
                return False
            return True
        except Exception:
            return False

    async def _recover_from_failure(self, reason: Optional[str] = None) -> bool:
        try:
            return await self._restart_from_beginning(reason)
        except Exception:
            return False

    async def _restart_from_beginning(self, reason: Optional[str] = None) -> bool:
        try:
            self.failure_count += 1
            self.logger.warning(f"Restarting session due to: {reason}")

            # If persistent, try to recover without killing the browser process
            if self.persistent and self.context:
                try:
                    self.logger.info("Persistent mode: Attempting soft restart (new page only)...")
                    if self.page:
                        try:
                            await self.page.close()
                        except Exception:
                            pass
                    
                    # Create new page
                    self.page = await self.context.new_page()
                    
                    # Verify browser is still responsive
                    await self.page.evaluate("1+1")
                    
                    # Login/Navigate check
                    if not await self.login(allow_restart=False):
                        raise Exception("Login failed during soft restart")
                        
                    self.logger.info("Soft restart successful.")
                    return True
                except Exception as e:
                    self.logger.error(f"Soft restart failed: {e}. Proceeding to full restart.")
                    # Fall through to full restart
                    try:
                         if self.context:
                             await self.context.close()
                    except Exception:
                        pass

            # Aggressive cleanup before restart
            try:
                if self.page:
                    try:
                        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                        fn = f"error_{ts}.png"
                        await self.page.screenshot(path=fn)
                    except Exception:
                        pass
                    try:
                        await self.page.close()
                    except Exception:
                        pass
            except Exception:
                pass
                
            try:
                await self._safe_close()
                await asyncio.sleep(2) # Allow OS to reclaim resources
            except Exception:
                pass
                
            # Clear internal state
            self.playwright = None
            self.browser = None
            self.context = None
            self.page = None
            
            ok = await self.initialize_browser()
            if not ok:
                try:
                    self.logger.error("Browser reinitialization failed during restart")
                except Exception:
                    pass
                return False
                
            # Do NOT clear cookies here if we want to persist the session!
            # The initialize_browser method has already loaded the saved state.
            # Clearing them now would force a re-login and trigger 2FA rate limits.
            # if self.context:
            #    try:
            #        await self.context.clear_cookies()
            #    except Exception:
            #        pass
            
            if self.page:
                try:
                    await self.page.evaluate("() => { try{sessionStorage.clear()}catch(e){} }")
                except Exception:
                    pass
                    
            ok = await self.login(allow_restart=False)
            if not ok:
                try:
                    self.logger.error("Login failed during restart")
                except Exception:
                    pass
                return False
            try:
                # Check if we are already on the bookings page (likely from login)
                if self.page and "bookings" in self.page.url:
                    self.logger.info("Already on bookings page after restart/login.")
                else:
                    url = "https://supplier.getyourguide.com/bookings"
                    if self.managed_by:
                        url += f"?managed_by={self.managed_by}"
                    await self.page.goto(url, wait_until="domcontentloaded")
                
                try:
                    await self.page.wait_for_load_state('networkidle', timeout=5000)
                except Exception:
                    await asyncio.sleep(1)
            except Exception as e:
                try:
                    self.logger.error(f"Failed to open bookings page after restart: {e}")
                except Exception:
                    pass
                return False
            try:
                self.logger.error(f"Session restarted due to: {reason}")
            except Exception:
                pass
            self.current_page = 1
            self.session_counter = 0
            if self.failure_count > self.max_retries:
                try:
                    self.logger.error(f"Exceeded max recovery retries: {self.max_retries}")
                except Exception:
                    pass
                await asyncio.sleep(self.retry_backoff_sec)
                self.failure_count = 0
            return True
        except Exception:
            return False

class SafeRotatingFileHandler(RotatingFileHandler):
    def doRollover(self):
        try:
            super().doRollover()
        except (PermissionError, OSError):
            pass

def setup_logging() -> logging.Logger:
    log_file = os.getenv("LOG_FILE", "gyg_unified.log")
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger("GYG_UNIFIED")
    logger.setLevel(getattr(logging, level, logging.INFO))
    logger.handlers.clear()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    try:
        fh = SafeRotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    except Exception:
        # Fallback if file cannot be opened
        fh = logging.StreamHandler()
        
    fh.setLevel(getattr(logging, level, logging.INFO))
    fh.setFormatter(formatter)
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, level, logging.INFO))
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def json_safedump(obj: Dict) -> str:
    try:
        import json
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"

def quote_table(name: str) -> str:
    try:
        from urllib.parse import quote
        return quote(name)
    except Exception:
        return name

def ai_enhance(api_key: str, text: str, breakdown: str, current: Dict) -> Dict:
    try:
        prompt = (
            "Given booking card content and price breakdown, extract corrected fields as JSON. "
            "Focus on extracting 'add_ons' which are extra items, fees, or services listed with prices (e.g. 'Entry Fee', 'Pickup', 'Lunch') that are NOT standard adult/child/student participants. "
            "If the text contains a list like 'Adult - €100.00', treat it as an add-on if the context implies it's a fee, otherwise check if it's a participant count. "
            "Only output JSON with keys: trip_name, destination, option_selected, date_trip, total_price_eur, "
            "retail_price, revenue, commission_breakdown, customer_name, customer_email, customer_phone, "
            "hotel_name, guide, adt, std, chd, inf, youth, add_ons (string)."
        )
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"text:\n{text}\n\nbreakdown:\n{breakdown}\n\ncurrent:\n{json_safedump(current)}"}
            ],
            "temperature": 0.2
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        r = requests.post("https://api.deepseek.com/v1/chat/completions", headers=headers, json=payload, timeout=30)
        if r.status_code != 200:
            return {}
        data = r.json() or {}
        content = (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or "{}"
        
        # Clean up markdown code blocks if present
        if "```" in content:
            # Try to find json block
            m = re.search(r'```json\s*([\s\S]*?)\s*```', content)
            if m:
                content = m.group(1)
            else:
                m = re.search(r'```\s*([\s\S]*?)\s*```', content)
                if m:
                    content = m.group(1)
        
        import json as _json
        try:
            res = _json.loads(content)
            return res if isinstance(res, dict) else {}
        except Exception:
            return {}
    except Exception:
        return {}

def _norm_region(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = s.strip().lower()
    if t in ("hurghada",):
        return "Hurghada"
    if t in ("cairo",):
        return "Cairo"
    if t in ("giza",):
        return "Giza"
    if t in ("luxor",):
        return "Luxor"
    if t in ("aswan",):
        return "Aswan"
    if t in ("alexandria",):
        return "Alexandria"
    if t in ("marsa alam", "marsaalam"):
        return "Marsa Alam"
    if t in ("el gouna", "elgouna"):
        return "El Gouna"
    if t in ("sharm el-sheikh", "sharm el sheikh", "sharm"):
        return "Sharm El-Sheikh"
    if t in ("nile cruise", "nilecruise"):
        return "Nile Cruise"
    return None

def extract_region(trip_name: Optional[str], option_selected: Optional[str], card_text: str) -> Optional[str]:
    # Specific overrides as requested
    des_overrides = {
        "Sahl Hasheesh Elite Beach Dive & Coral Reef Experience": "Hurghada",
        "Marsa Mubarak Sea Turtles Trip with Optional Diving": "Marsa Alam"
    }
    
    trip_name_clean = (trip_name or "").strip()
    if trip_name_clean in des_overrides:
        return des_overrides[trip_name_clean]

    if trip_name and ":" in trip_name:
        r = trip_name.split(":", 1)[0].strip()
        n = _norm_region(r)
        if n:
            return n
    candidates = [trip_name or "", option_selected or "", card_text or ""]
    words = ["Hurghada","Cairo","Giza","Luxor","Aswan","Alexandria","Marsa Alam","El Gouna","Sharm El-Sheikh","Sharm El Sheikh","Sharm","Nile Cruise"]
    low = " ".join(candidates).lower()
    for w in words:
        if w.lower() in low:
            n = _norm_region(w)
            if n:
                return n
    return None

def _parse_date_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = s.strip()
    for suf in ["st", "nd", "rd", "th"]:
        t = re.sub(rf"\b(\d+)\s*{suf}\b", r"\1", t)
    m = re.search(r"([A-Za-z]+),\s+([A-Za-z]+)\s+(\d+),\s+(\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)", t)
    if not m:
        return None
    wk, mon, day, year, hh, mm, ap = m.groups()
    month_map = {
        "January":1,"February":2,"March":3,"April":4,"May":5,"June":6,
        "July":7,"August":8,"September":9,"October":10,"November":11,"December":12
    }
    mi = month_map.get(mon, 0)
    if mi == 0:
        return None
    h = int(hh)
    if ap.upper() == "AM":
        h = 0 if h == 12 else h
    else:
        h = 12 if h == 12 else h + 12
    try:
        # If GYG says 20:30, we want it to display EXACTLY as 20:30 in Airtable.
        # Since Airtable field "Date Trip" is configured to show "Local Time" (EEST/UTC+3 or UTC+2),
        # the most foolproof way to bypass all Airtable timezone conversions is to format the date
        # as a raw ISO string without ANY timezone indicator (no 'Z', no offset).
        # This forces Airtable to treat it as "floating local time" and display it exactly as string.
        dt = datetime(int(year), mi, int(day), h, int(mm))
        return dt.strftime('%Y-%m-%dT%H:%M:00')
    except Exception:
        return None

def _sanitize_commission(v: Optional[object]) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        val = float(v)
        # If it's already a percentage value (like 32.0), return as-is
        # Only multiply by 100 if it's a decimal (like 0.32)
        return val if val <= 100.0 else val / 100.0
    s = str(v).strip()
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%", s)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    try:
        val = float(s)
        # If it's already a percentage value (like 32.0), return as-is
        # Only multiply by 100 if it's a decimal (like 0.32)
        return val if val <= 100.0 else val / 100.0
    except Exception:
        return None

def _sanitize_booking_nr(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    t = re.sub(r"[^A-Za-z0-9]", "", s.upper())
    if not t.startswith("GYG"):
        return None
    # Enforce 12-character total booking code (common format: GYG + 9 chars)
    if len(t) > 12:
        t = t[:12]
    return t if len(t) >= 10 else None

async def _amain(args: List[str]):
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--server", action="store_true")
    p.add_argument("--once", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--single-page", action="store_true")
    p.add_argument("--test-pages", type=int, help="Test extraction from multiple pages (specify number of pages)", default=0)
    p.add_argument("--resync-nrs", type=str, help="Comma-separated booking numbers to resync", default="")
    a = p.parse_args(args)
    sys_ok = bool(os.getenv("GYG_EMAIL")) and bool(os.getenv("GYG_PASSWORD"))
    if a.dry_run:
        print("OK" if sys_ok else "MISSING_ENV")
        return
    system = GYGUnifiedSystem()
    if a.once:
        system.run_once_flag = True
    if a.server:
        await system.run_server()
    else:
        if a.single_page:
            orig = system.run_extraction
            async def run_extraction_single_page():
                url = "https://supplier.getyourguide.com/bookings"
                if system.managed_by:
                    url += f"?managed_by={system.managed_by}"
                await system.page.goto(url, wait_until="load")
                await asyncio.sleep(2)
                bookings = await system.extract_bookings_from_page()
                total_synced = 0
                total_failed = 0
                for b in bookings:
                    system.logger.info(f"Syncing booking {b.get('booking_nr')} ...")
                    if await system.sync_booking(b):
                        total_synced += 1
                    else:
                        total_failed += 1
                system.logger.info(f"Single-page results: synced={total_synced} failed={total_failed}")
                return {"synced": total_synced, "failed": total_failed}
            system.run_extraction = run_extraction_single_page
        elif a.resync_nrs:
            targets = {s.strip() for s in a.resync_nrs.split(',') if s.strip()}
            async def run_extraction_resync_specific():
                url = "https://supplier.getyourguide.com/bookings"
                if system.managed_by:
                    url += f"?managed_by={system.managed_by}"
                await system.page.goto(url, wait_until="load")
                await asyncio.sleep(2)
                total_synced = 0
                total_failed = 0
                page_num = 1
                while True:
                    bookings = await system.extract_bookings_from_page()
                    if not bookings:
                        break
                    for b in bookings:
                        if b.get('booking_nr') in targets:
                            system.logger.info(f"Resync target {b.get('booking_nr')} detected on page {page_num}")
                            if await system.sync_booking(b):
                                total_synced += 1
                            else:
                                total_failed += 1
                    next_btn = await system.page.query_selector('button:has-text("Next")')
                    if not next_btn or not await system.page.is_enabled('button:has-text("Next")'):
                        break
                    await next_btn.click()
                    await asyncio.sleep(2)
                    page_num += 1
                system.logger.info(f"Resync results: synced={total_synced} failed={total_failed}")
                return {"synced": total_synced, "failed": total_failed}
            system.run_extraction = run_extraction_resync_specific
        elif a.test_pages > 0:
            # Test mode: extract from specified number of pages
            orig = system.run_extraction
            async def run_extraction_test_pages():
                url = "https://supplier.getyourguide.com/bookings"
                if system.managed_by:
                    url += f"?managed_by={system.managed_by}"
                await system.page.goto(url, wait_until="load")
                await asyncio.sleep(2)
                
                total_synced = 0
                total_failed = 0
                page_num = 1
                
                # Get total pages available
                total_pages = await system._get_total_pages()
                system.logger.info(f"Test mode: will extract from up to {min(a.test_pages, total_pages)} pages out of {total_pages} total")
                
                while page_num <= min(a.test_pages, total_pages):
                    system.logger.info(f"Test extracting from page {page_num} of {min(a.test_pages, total_pages)}")
                    
                    # Extract from current page
                    bookings = await system.extract_bookings_from_page()
                    if not bookings:
                        system.logger.warning(f"No bookings found on page {page_num}")
                        break
                        
                    for b in bookings:
                        system.logger.info(f"Test sync booking {b.get('booking_nr')} from page {page_num}")
                        if await system.sync_booking(b):
                            total_synced += 1
                        else:
                            total_failed += 1
                    
                    # Navigate to next page
                    if page_num < min(a.test_pages, total_pages):
                        if not await system._navigate_to_page(page_num + 1):
                            system.logger.warning(f"Failed to navigate to page {page_num + 1}")
                            break
                        await asyncio.sleep(1.5)
                    
                    page_num += 1
                    
                system.logger.info(f"Test results: synced={total_synced} failed={total_failed} from {page_num-1} pages")
                return {"synced": total_synced, "failed": total_failed, "pages_processed": page_num-1}
            system.run_extraction = run_extraction_test_pages
        await system.run_once()

if __name__ == "__main__":
    import sys
    try:
        asyncio.run(_amain(sys.argv[1:]))
    except KeyboardInterrupt:
        pass
        
