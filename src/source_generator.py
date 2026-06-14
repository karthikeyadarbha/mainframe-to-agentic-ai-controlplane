import os
import struct

def encode_comp3(number: int, length: int) -> bytes:
    """
    Purpose:
        Packs an integer into a standard COBOL COMP-3 packed decimal format.
        Pairs two numeric digits per byte, finalizing with a sign nibble (C = Positive, D = Negative).
    Input:
        number (int): The plain integer value to be compressed.
        length (int): The target byte length of the resulting packed array.
    Output:
        bytes: The raw, COMP-3 encoded binary byte sequence.
    Execution Outcome:
        Converts a standard integer into hexadecimal nibbles, pads it with 
        leading zeros, sets the sign nibble, and returns the exact byte slice.
    """
    sign = 0xC  # Positive
    num_str = f"{abs(number):x}"
    if len(num_str) % 2 == 0:
        num_str = "0" + num_str
    
    hex_str = num_str + f"{sign:x}"
    binary_data = bytes.fromhex(hex_str)
    
    if len(binary_data) < length:
        binary_data = b'\x00' * (length - len(binary_data)) + binary_data
    return binary_data[-length:]

def generate_high_volume_ebcdic_source(output_path: str, record_count: int = 10000):
    """
    Purpose:
        Synthesizes a large, realistic binary file mimicking a high-concurrency 
        core-banking batch extraction (e.g., Finacle). Encodes strings via EBCDIC (cp500) 
        and packs numerical balances using binary COMP-3, injecting controlled structural 
        and semantic anomalies.
    Input:
        output_path (str): The local target destination path for the generated binary flat file.
        record_count (int): The total number of ledger records to synthesize (default: 10,000).
    Output:
        None (Materializes a physical binary file to disk).
    Execution Outcome:
        Iterates up to the `record_count`, generating alternating valid distributions 
        and malformed records (e.g., blank keys, negative signs), writing a continuous, 
        uncompressed stream of 35-byte physical records directly to the landing zone.
    """
    print(f"📡 [MAINFRAME COMPUTE] Synthesizing {record_count} high-volume Finacle EBCDIC ledger records...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, "wb") as f:
        for i in range(1, record_count + 1):
            tx_id = f"TXN{202600000 + i}".encode('cp500') # EBCDIC text (12 bytes)
            
            # Inject structural anomaly (blank customer ID)
            if i % 2500 == 0:
                cust_id = "          ".encode('cp500')  # Malformed blank ID (10 bytes)
                balance_raw = encode_comp3(50000, 5)     # Comp-3 ($500.00) (5 bytes)
                status = "ACTIVE  ".encode('cp500')      # EBCDIC text (8 bytes)
            # Inject semantic anomaly (force negative sign nibble 'D')
            elif i % 3333 == 0:
                cust_id = f"CUST{i:06d}".encode('cp500')
                balance_raw = b'\x00\x00\x00\x50\x0D'    # Negative balance (-$500.00)
                status = "ACTIVE  ".encode('cp500')
            else:
                cust_id = f"CUST{990000 + (i % 100):06d}".encode('cp500')
                balance_raw = encode_comp3(550000000 + (i * 100), 5) # Balances around $5.5M
                status = "ACTIVE  ".encode('cp500') if i % 5000 != 0 else "SUSPEND ".encode('cp500')

            f.write(tx_id + cust_id + balance_raw + status)
            
    print(f"💾 [STORAGE] SOTA Finacle output successfully written to: {output_path} ({record_count * 35} bytes)")

if __name__ == "__main__":
    generate_high_volume_ebcdic_source("data/landing/FINACLE_LEDGER_DUMP.bin", record_count=10000)