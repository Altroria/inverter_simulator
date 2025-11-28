#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
goodwe_inventerinfo_test.py

功能:
- pub: 发布设备回复消息到指定主题（默认 /goodwe/ccm/client/inventerinfo）
- sub: 订阅任意主题查看消息
- handle: 订阅平台下发（默认 /goodwe/ccm/server/ccmset/72101WLA25403979），
          解析 JSON（兼容 wrapper），自动生成设备回复并发布到上行主题
          （默认 /goodwe/ccm/client/inventerinfo）。

本版新增（handle 模式）:
- 动态生成应答 Order：
  1) 若下发为 Read Holding Registers (0x03) 请求帧: F7 03 <addr><qty> ... →
     回复 Write Multiple Registers (0x10) 确认帧: F7 10 <addr><qty> + CRC(LE)
  2) 若下发为 Write Multiple Registers (0x10) 请求帧(带数据): F7 10 <addr><qty> <byteCount> <data...> →
     回复标准确认帧: F7 10 <addr><qty> + CRC(LE)
- 若无法解析出应答帧, 回退使用 --ack-order 固定值。
- MessageType: 默认将 41 → 42；MessageId 默认生成新的（与示例一致），可扩展镜像策略。

依赖:
  pip install paho-mqtt
"""
import argparse
import json
import time
import uuid
import ssl
from typing import Optional, Any, Dict, Tuple
from paho.mqtt import client as mqtt

# -------------------------- CRC/解析工具 --------------------------

def crc16_modbus(data: bytes) -> int:\n    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


def robust_json_loads(s: str) -> Any:
    """兼容双重编码(JSON 字符串内再嵌套 JSON 字符串)。"""
    try:
        obj = json.loads(s)
        if isinstance(obj, str):
            try:
                return json.loads(obj)
            except Exception:
                return obj
        return obj
    except Exception:
        return {}


def parse_downlink(raw_bytes: bytes) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    输入原始 MQTT payload，输出两个 dict：
    - meta: 如果是 wrapper（含 clientid/topic 等）则返回相关字段，否则为空
    - dl:   解析后的下发业务 JSON（应包含 CcmSn/MessageType/Data 等）
    """
    text = raw_bytes.decode("utf-8", errors="ignore")
    obj = robust_json_loads(text)
    meta: Dict[str, Any] = {}
    dl: Dict[str, Any] = {}
    if isinstance(obj, dict) and "clientid" in obj and "payload" in obj:
        meta = obj
        inner = obj.get("payload", "")
        dl = robust_json_loads(inner) if isinstance(inner, str) else inner
    else:
        dl = obj if isinstance(obj, dict) else {}
    return meta, dl


def map_order_read03_to_write10_ack(order_hex: str) -> str:
    """
    将下发的 F7 03 <StartHi><StartLo><QtyHi><QtyLo> [CRC] 映射为
    F7 10 <StartHi><StartLo><QtyHi><QtyLo> [CRC] 的确认帧。
    """
    if not isinstance(order_hex, str):
        return ""
    h = order_hex.strip().upper()
    try:
        raw = bytes.fromhex(h)
    except Exception:
        return ""
    # 最少: 地址(1)+功能(1)+起始(2)+数量(2)+CRC(2) = 8字节
    if len(raw) < 8:
        return ""
    payload = raw[:-2]  # 去掉请求CRC
    if len(payload) < 6:
        return ""
    addr = payload[0]
    func = payload[1]
    if func != 0x03:
        return ""
    start_hi, start_lo, qty_hi, qty_lo = payload[2], payload[3], payload[4], payload[5]
    resp_wo_crc = bytes([addr, 0x10, start_hi, start_lo, qty_hi, qty_lo])
    crc = crc16_modbus(resp_wo_crc)
    resp = resp_wo_crc + crc.to_bytes(2, "little")  # 小端附加
    return resp.hex().upper()


def map_order_write10_to_ack(order_hex: str) -> str:
    """
    对 Write Multiple Registers 请求帧生成确认帧:
    请求: F7 10 <StartHi><StartLo><QtyHi><QtyLo><ByteCnt><Data...> [CRC]
    回复: F7 10 <StartHi><StartLo><QtyHi><QtyLo> [CRC]
    """
    if not isinstance(order_hex, str):
        return ""
    h = order_hex.strip().upper()
    try:
        raw = bytes.fromhex(h)
    except Exception:
        return ""
    if len(raw) < 9:  # 至少包含 ByteCount 一个字节
        return ""
    payload = raw[:-2]
    if len(payload) < 7:
        return ""
    addr = payload[0]
    func = payload[1]
    if func != 0x10:
        return ""
    start_hi, start_lo, qty_hi, qty_lo = payload[2], payload[3], payload[4], payload[5]
    # 可选: 校验 byteCount 与 data 长度，这里放宽
    resp_wo_crc = bytes([addr, 0x10, start_hi, start_lo, qty_hi, qty_lo])
    crc = crc16_modbus(resp_wo_crc)
    resp = resp_wo_crc + crc.to_bytes(2, "little")
    return resp.hex().upper()


def compute_ack_order(in_order: str, fallback: str) -> str:
    """按 0x03/0x10 规则生成 ACK；失败则返回 fallback。"""
    out = map_order_read03_to_write10_ack(in_order)
    if out:
        return out
    out = map_order_write10_to_ack(in_order)
    if out:
        return out
    return fallback


def wrap_uplink(reply: Dict[str, Any], ccm_sn: str) -> Dict[str, Any]:
    """设备上报包装格式（可选）。"""
    return {
        "CompressFlag": "0",
        "InventerSN": ccm_sn,
        "ReturnMessage": json.dumps(reply, separators=(",", ":"), ensure_ascii=False),
        "Type": 1,
    }

# -------------------------- MQTT 客户端 --------------------------

def make_client(args, client_id_prefix="gw-test"):
    cid = args.client_id or f"{client_id_prefix}-{uuid.uuid4().hex[:6]}"
    cli = mqtt.Client(client_id=cid, protocol=mqtt.MQTTv311)
    if args.username:
        cli.username_pw_set(args.username, args.password or "")
    if args.tls:
        # 若未提供证书文件, 使用系统默认 CA
        if args.ca or args.cert or args.key:
            cli.tls_set(ca_certs=args.ca, certfile=args.cert, keyfile=args.key)
        else:
            cli.tls_set()
        if args.insecure:
            cli.tls_insecure_set(True)
    return cli

# -------------------------- 模式: 发布 --------------------------

def build_payload(
    message_type: int,
    ccm_sn: str,
    device_sn: str,
    order_hex: str,
    message_id: Optional[str] = None,
) -> dict:
    return {
        "MessageType": message_type,
        "CcmSn": ccm_sn,
        "MessageId": message_id or str(uuid.uuid4()),
        "Data": [
            {
                "DeviceSn": device_sn,
                "Order": order_hex
            }
        ]
    }


def publish_mode(args):
    payload = build_payload(
        message_type=args.message_type,
        ccm_sn=args.ccm_sn,
        device_sn=args.device_sn,
        order_hex=args.order,
        message_id=args.message_id,
    )
    cli = make_client(args, "gw-pub")
    cli.connect(args.host, args.port, keepalive=60)
    cli.loop_start()
    try:
        for i in range(args.repeat):
            data_to_pub: Any = payload
            if args.wrap_up:
                data_to_pub = wrap_uplink(payload, args.ccm_sn)
            msg = json.dumps(data_to_pub, ensure_ascii=False, separators=(",", ":"))
            info = cli.publish(args.topic, msg, qos=args.qos, retain=False)
            info.wait_for_publish()
            print(f"[PUBLISHED] {args.topic} qos={args.qos} MessageId={payload['MessageId']} ({i+1}/{args.repeat})")
            if i < args.repeat - 1:
                time.sleep(args.interval)
                if args.rotate_msgid:
                    payload["MessageId"] = str(uuid.uuid4())
    finally:
        cli.loop_stop()
        cli.disconnect()

# -------------------------- 模式: 订阅 --------------------------

def subscribe_mode(args):
    cli = make_client(args, "gw-sub")

    def on_connect(client, userdata, flags, rc):
        print(f"[CONNECTED] rc={rc}, subscribing {args.topic}")
        client.subscribe(args.topic, qos=args.qos)

    def on_message(client, userdata, msg):
        try:
            print(f"[RECV] {msg.topic} qos={msg.qos}\n{msg.payload.decode('utf-8')}\n")
        except Exception as e:
            print(f"[RECV-ERR] {e}")

    cli.on_connect = on_connect
    cli.on_message = on_message
    cli.connect(args.host, args.port, keepalive=60)
    try:
        cli.loop_forever()
    except KeyboardInterrupt:
        cli.disconnect()

# -------------------------- 模式: 处理下发并回包 --------------------------
class IdemCache:
    def __init__(self, limit: int = 512):
        self._data: Dict[str, Dict[str, Any]] = {}
        self._limit = limit

    def get(self, key: str):
        return self._data.get(key)

    def put(self, key: str, val: Dict[str, Any]):
        if len(self._data) >= self._limit:
            self._data.clear()
        self._data[key] = val


def handle_mode(args):
    cli = make_client(args, "gw-handle")
    cache = IdemCache(limit=512)

    sub_topic = args.sub_topic
    pub_topic = args.pub_topic

    def on_connect(client, userdata, flags, rc):
        print(f"[CONNECTED] rc={rc}, subscribing {sub_topic}")
        client.subscribe(sub_topic, qos=args.qos)

    def on_message(client, userdata, msg):
        try:
            meta, dl = parse_downlink(msg.payload)
            # 解析 CcmSn、MessageType、MessageId、DeviceSn、Order
            ccm_sn = dl.get("CcmSn") or dl.get("CcmSN") or dl.get("ccmSn") or ""
            message_type = dl.get("MessageType")
            message_id = dl.get("MessageId") or str(uuid.uuid4())

            device_sn = args.device_sn
            in_order = ""
            data_arr = dl.get("Data")
            if isinstance(data_arr, list) and data_arr:
                first = data_arr[0] or {}
                device_sn = first.get("DeviceSn") or device_sn
                in_order = first.get("Order") or ""

            print(f"[DOWNLINK] topic={msg.topic} CcmSn={ccm_sn} MessageType={message_type} MessageId={message_id} Order={in_order}")
            if meta:
                brief = {k: meta.get(k) for k in ["clientid", "topic", "qos", "retain", "transferBridgeDate"] if k in meta}
                print(f"[DOWNLINK-META] {brief}")

            # 幂等: 相同 MessageId 返回相同应答
            cached = cache.get(message_id)
            if cached:
                data_to_pub = cached
            else:
                # 生成 ACK 的 Order（优先动态映射，其次回退）
                ack_order = compute_ack_order(in_order, args.ack_order)
                reply_type = 42 if message_type == 41 else args.reply_msgtype
                reply = build_payload(
                    message_type=reply_type,
                    ccm_sn=ccm_sn or args.ccm_sn,
                    device_sn=device_sn,
                    order_hex=ack_order,
                    message_id=str(uuid.uuid4()),  # 默认生成新的 MessageId（与示例一致）
                )
                data_to_pub = wrap_uplink(reply, ccm_sn or args.ccm_sn) if args.wrap_up else reply
                cache.put(message_id, data_to_pub)

            payload_str = json.dumps(data_to_pub, ensure_ascii=False, separators=(",", ":"))
            info = client.publish(pub_topic, payload_str, qos=args.qos, retain=False)
            info.wait_for_publish()
            print(f"[REPLY] -> {pub_topic} qos={args.qos}")
        except Exception as e:
            print(f"[ERR] {e}")

    cli.on_connect = on_connect
    cli.on_message = on_message
    cli.connect(args.host, args.port, keepalive=60)
    try:
        cli.loop_forever()
    except KeyboardInterrupt:
        cli.disconnect()

# -------------------------- 入口 --------------------------

def main():
    ap = argparse.ArgumentParser(description="GoodWe inventerinfo tester (pub/sub/handle)")
    ap.add_argument("--mode", choices=["pub", "sub", "handle"], default="handle", help="pub: 发布; sub: 订阅; handle: 订阅下发并自动回包")

    # Broker
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--username", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--client-id", default=None, help="自定义 MQTT ClientID")
    ap.add_argument("--tls", action="store_true", help="启用 TLS(通常端口为 8883)")
    ap.add_argument("--ca", default=None, help="CA 文件路径")
    ap.add_argument("--cert", default=None, help="客户端证书路径")
    ap.add_argument("--key", default=None, help="客户端私钥路径")
    ap.add_argument("--insecure", action="store_true", help="忽略证书校验(测试用)")

    # 主题/QoS（pub/sub）
    ap.add_argument("--topic", default="/goodwe/ccm/client/inventerinfo", help="发布/订阅主题 (pub/sub 模式)")
    ap.add_argument("--qos", type=int, default=0, choices=[0, 1, 2])

    # 业务载荷（pub 模式）
    ap.add_argument("--message-type", type=int, default=42)
    ap.add_argument("--ccm-sn", default="72101WLA25403979")
    ap.add_argument("--device-sn", default="96000NAH256L8002")
    ap.add_argument("--order", default="F710A68D0001A7FC")
    ap.add_argument("--message-id", default=None, help="不传则自动生成 UUID")

    # 批量/节流（pub 模式）
    ap.add_argument("--repeat", type=int, default=1, help="重复发送次数（pub）")
    ap.add_argument("--interval", type=float, default=1.0, help="重复发送间隔秒（pub）")
    ap.add_argument("--rotate-msgid", action="store_true", help="每次发送更换 MessageId（pub）")

    # handle 模式参数（根据你的要求设置默认主题）
    ap.add_argument("--sub-topic", default="/goodwe/ccm/server/ccmset/72101WLA25403979", help="下发订阅主题 (handle 模式)")
    ap.add_argument("--pub-topic", default="/goodwe/ccm/client/inventerinfo", help="回复发布主题 (handle 模式)")
    ap.add_argument("--reply-msgtype", type=int, default=42, help="默认回复 MessageType（非 41 映射情况）")
    ap.add_argument("--ack-order", default="F703020126F01B", help="无法解析时使用的固定 Order")
    ap.add_argument("--wrap-up", action="store_true", help="将上行包包装为 {CompressFlag, InventerSN, ReturnMessage, Type}")

    args = ap.parse_args()

    if args.mode == "pub":
        publish_mode(args)
    elif args.mode == "sub":
        subscribe_mode(args)
    else:
        handle_mode(args)


if __name__ == "__main__":
    main()
