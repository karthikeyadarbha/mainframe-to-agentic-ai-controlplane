import os
import struct

def encode_comp3(number: int, length: int) -> bytes:
    """
    SOTA Emulator: Packs an integer into a standard COBOL COMP-3 packed decimal format.
    Pairs two numeric digits per byte, finalizing with a sign nibble (C = Positive).
    """
    sign = 0xC  # Positive
    num_str = f"{abs(number):x}"
    if len(num_str) % 2 == 0:
        num_str = "0" + num_str
    
    # Ensure the string ends with the sign nibble
    hex_str = num_str + f"{sign:x}"
    binary_data = bytes.fromhex(hex_str)
    
    # Pad with leading zeros to match expected fixed length bytes
    if len(binary_data) < length:
        binary_data = b'\x00' * (length - len(binary_data)) + binary_data
    return binary_data[-length:]

def generate_high_volume_ebcdic_source(output_path: str, record_count: int = 10000):
    """
    Generates a true binary file mimicking a massive Finacle core extraction dump.
    Uses EBCDIC (cp500) encoding for strings and binary COMP-3 for decimal balances.
    """
    print(f"📡 [MAINFRAME COMPUTE] Synthesizing {record_count} high-volume Finacle EBCDIC ledger records...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, "wb") as f:
        for i in range(1, record_count + 1):
            # Formulating highly concurrent account distributions
            tx_id = f"TXN{202600000 + i}".encode('cp500') # EBCDIC text (12 bytes)
            
            # Inject intentional structural anomalies every 2500 records
            if i % 2500 == 0:
                cust_id = "          ".encode('cp500')  # Malformed blank ID (10 bytes)
                balance_raw = encode_comp3(50000, 5)     # Comp-3 ($500.00) (5 bytes)
                status = "ACTIVE  ".encode('cp500')      # EBCDIC text (8 bytes)
            # Inject intentional semantic anomalies every 3333 records
            elif i % 3333 == 0:
                cust_id = f"CUST{i:06d}".encode('cp500')
                balance_raw = b'\x00\x00\x00\x50\x0D'    # Force Negative sign nibble 'D' (-$500.00)
                status = "ACTIVE  ".encode('cp500')
            else:
                cust_id = f"CUST{990000 + (i % 100):06d}".encode('cp500') # Regular HNW distribution
                balance_raw = encode_comp3(550000000 + (i * 100), 5)     # HNW Balances around $5.5M
                status = "ACTIVE  ".encode('cp500') if i % 5000 != 0 else "SUSPEND ".encode('cp500')

            # Write standard 35-byte binary block layout directly to disk
            f.write(tx_id + cust_id + balance_raw + status)
            
    print(f"💾 [STORAGE] SOTA Finacle output successfully written to: {output_path} ({record_count * 35} bytes)")

if __name__ == "__main__":
    generate_high_volume_ebcdic_source("data/landing/FINACLE_LEDGER_DUMP.bin", record_count=10000)