#!/usr/bin/env python3
"""检查 spv_internal_params 表及 raw_loan 数据"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    try:
        from db_connect import get_connection
        conn = get_connection()
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    cur = conn.cursor()
    try:
        # 检查 spv_internal_params
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'spv_internal_params'
            )
        """)
        has_internal = cur.fetchone()[0]
        print(f"spv_internal_params 表存在: {has_internal}")

        if has_internal:
            cur.execute("SELECT * FROM spv_internal_params LIMIT 5")
            rows = cur.fetchall()
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'spv_internal_params' ORDER BY ordinal_position")
            cols = [r[0] for r in cur.fetchall()]
            print(f"  列: {cols}")
            print(f"  行数(前5): {len(rows)}")
            for r in rows:
                print(f"    {dict(zip(cols, r))}")
            cur.execute("SELECT DISTINCT spv_id FROM spv_internal_params")
            spv_ids = [r[0] for r in cur.fetchall()]
            print(f"  spv_id 列表: {spv_ids}")

        # 检查 raw_loan
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'raw_loan'
            )
        """)
        has_raw_loan = cur.fetchone()[0]
        print(f"\nraw_loan 表存在: {has_raw_loan}")

        if has_raw_loan:
            cur.execute("SELECT DISTINCT spv_id FROM raw_loan LIMIT 20")
            spv_ids_loan = [r[0] for r in cur.fetchall()]
            print(f"  raw_loan 中的 spv_id 示例: {spv_ids_loan}")
            cur.execute("""
                SELECT spv_id, COUNT(*), COALESCE(SUM(disbursement_amount), 0), COUNT(DISTINCT customer_id)
                FROM raw_loan WHERE disbursement_amount IS NOT NULL
                GROUP BY spv_id
            """)
            for r in cur.fetchall():
                print(f"    {r[0]}: 笔数={r[1]}, 放款总额(本币)={r[2]:,.0f}, 借款人数={r[3]}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
