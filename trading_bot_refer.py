"""
模块化交易机器人 - 支持多个交易所
"""

import os
import time
import asyncio
import traceback
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.lark_bot import LarkBot


@dataclass
class TradingConfig:
    """交易参数配置类 - 存储所有交易相关的配置参数"""

    ticker: str  # 交易标的符号（如：ETH, BTC, SOL）
    contract_id: str  # 合约ID（由交易所返回，自动解析）
    quantity: Decimal  # 每笔订单的交易数量
    take_profit: Decimal  # 止盈百分比（如：0.02 表示 0.02%）
    tick_size: Decimal  # 价格最小变动单位（由交易所返回）
    direction: str  # 交易方向：'buy'（做多）或 'sell'（做空）
    max_orders: int  # 最大活跃订单数量（风险控制）
    wait_time: int  # 订单之间的等待时间（秒）
    exchange: str  # 交易所名称：'edgex', 'backpack', 'paradex', 'aster', 'lighter'
    grid_step: Decimal  # 网格步长百分比，控制平仓订单的最小价格间距（-100表示无限制）
    stop_price: Decimal  # 停止交易价格（达到此价格时退出程序，-1表示不设置）
    pause_price: Decimal  # 暂停交易价格（达到此价格时暂停下单，-1表示不设置）
    aster_boost: bool  # 是否启用Aster交易所的Boost模式

    @property
    def close_order_side(self) -> str:
        """根据机器人交易方向获取平仓订单的方向

        如果做多(buy)，平仓方向是卖出(sell)
        如果做空(sell)，平仓方向是买入(buy)
        """
        return "buy" if self.direction == "sell" else "sell"


@dataclass
class OrderMonitor:
    """订单监控状态类 - 线程安全的订单监控状态管理"""

    order_id: Optional[str] = None  # 订单ID
    filled: bool = False  # 是否已完全成交
    filled_price: Optional[Decimal] = None  # 成交价格
    filled_qty: Decimal = 0.0  # 已成交数量

    def reset(self):
        """重置监控状态 - 清空所有订单信息"""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


class TradingBot:
    """模块化交易机器人 - 支持多个交易所的主要交易逻辑"""

    def __init__(self, config: TradingConfig):
        """初始化交易机器人

        参数:
            config: TradingConfig 交易配置对象，包含所有交易参数
        """
        self.config = config
        # 创建日志记录器，用于记录交易活动和错误信息
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # 创建交易所客户端 - 根据配置中的交易所名称创建对应的客户端
        try:
            self.exchange_client = ExchangeFactory.create_exchange(
                config.exchange, config
            )
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # 交易状态变量
        self.active_close_orders = []  # 当前活跃的平仓订单列表
        self.last_close_orders = 0  # 上一次检查时的平仓订单数量
        self.last_open_order_time = 0  # 上一次下开仓订单的时间戳
        self.last_log_time = 0  # 上一次记录日志的时间戳
        self.current_order_status = None  # 当前订单状态
        self.order_filled_event = asyncio.Event()  # 订单成交事件（异步事件）
        self.order_canceled_event = asyncio.Event()  # 订单取消事件（异步事件）
        self.shutdown_requested = False  # 是否请求关闭机器人
        self.loop = None  # asyncio事件循环引用

        # 注册WebSocket订单回调处理器
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """优雅关闭交易机器人

        参数:
            reason: 关闭原因（默认为"Unknown"）

        功能:
            1. 记录关闭原因
            2. 设置关闭标志位
            3. 断开与交易所的连接
        """
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # 断开与交易所的连接
            await self.exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")

    def _setup_websocket_handlers(self):
        """设置WebSocket订单更新处理器

        功能:
            注册一个回调函数来处理从交易所WebSocket接收到的订单更新消息
        """

        def order_update_handler(message):
            """处理来自WebSocket的订单更新消息

            参数:
                message: dict - 订单更新消息，包含订单ID、状态、价格等信息
            """
            try:
                # 检查是否是我们正在交易的合约
                if message.get("contract_id") != self.config.contract_id:
                    return

                # 从消息中提取订单信息
                order_id = message.get("order_id")  # 订单ID
                status = message.get(
                    "status"
                )  # 订单状态：FILLED/CANCELED/PARTIALLY_FILLED等
                side = message.get("side", "")  # 订单方向：buy/sell
                order_type = message.get(
                    "order_type", ""
                )  # 订单类型：OPEN(开仓)/CLOSE(平仓)
                filled_size = Decimal(message.get("filled_size"))  # 已成交数量

                # 如果是开仓订单，更新当前订单状态
                if order_type == "OPEN":
                    self.current_order_status = status

                # 处理完全成交的订单
                if status == "FILLED":
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        # 线程安全地触发订单成交事件（因为WebSocket回调可能在不同线程）
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_filled_event.set)
                        else:
                            # 后备方案（run()启动后不应该发生）
                            self.order_filled_event.set()

                    # 记录成交信息到日志
                    self.logger.log(
                        f"[{order_type}] [{order_id}] {status} "
                        f"{message.get('size')} @ {message.get('price')}",
                        "INFO",
                    )
                    self.logger.log_transaction(
                        order_id,
                        side,
                        message.get("size"),
                        message.get("price"),
                        status,
                    )

                # 处理已取消的订单
                elif status == "CANCELED":
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        # 线程安全地触发订单取消事件
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(
                                self.order_canceled_event.set
                            )
                        else:
                            self.order_canceled_event.set()

                        # 如果订单被取消前有部分成交，记录成交部分
                        if self.order_filled_amount > 0:
                            self.logger.log_transaction(
                                order_id,
                                side,
                                self.order_filled_amount,
                                message.get("price"),
                                status,
                            )

                    self.logger.log(
                        f"[{order_type}] [{order_id}] {status} "
                        f"{message.get('size')} @ {message.get('price')}",
                        "INFO",
                    )

                # 处理部分成交的订单
                elif status == "PARTIALLY_FILLED":
                    self.logger.log(
                        f"[{order_type}] [{order_id}] {status} "
                        f"{filled_size} @ {message.get('price')}",
                        "INFO",
                    )

                # 处理其他状态的订单
                else:
                    self.logger.log(
                        f"[{order_type}] [{order_id}] {status} "
                        f"{message.get('size')} @ {message.get('price')}",
                        "INFO",
                    )

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # 将订单更新处理器注册到交易所客户端
        self.exchange_client.setup_order_update_handler(order_update_handler)

    def _calculate_wait_time(self) -> Decimal:
        """计算订单之间的等待时间

        返回:
            0 - 可以立即下单
            1 - 需要等待（1秒后再检查）

        等待时间策略:
            - 当有平仓订单成交时（订单数减少），立即下单
            - 订单数量达到max_orders时，等待
            - 根据当前订单数占max_orders的比例动态调整冷却时间：
              * >= 2/3: 冷却时间 = 2 * wait_time（更保守）
              * >= 1/3: 冷却时间 = wait_time（正常）
              * >= 1/6: 冷却时间 = wait_time / 2（较激进）
              * < 1/6:  冷却时间 = wait_time / 4（最激进）
        """
        cool_down_time = self.config.wait_time

        # 如果订单数量减少了（说明有订单成交），立即下新单
        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0

        # 更新上次订单数量
        self.last_close_orders = len(self.active_close_orders)

        # 如果订单数量达到最大值，暂停下单
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1

        # 根据订单数量占比动态调整冷却时间
        if len(self.active_close_orders) / self.config.max_orders >= 2 / 3:
            cool_down_time = 2 * self.config.wait_time  # 订单较多，更保守
        elif len(self.active_close_orders) / self.config.max_orders >= 1 / 3:
            cool_down_time = self.config.wait_time  # 订单中等，正常策略
        elif len(self.active_close_orders) / self.config.max_orders >= 1 / 6:
            cool_down_time = self.config.wait_time / 2  # 订单较少，稍激进
        else:
            cool_down_time = self.config.wait_time / 4  # 订单很少，最激进

        # 如果程序启动时检测到已有平仓订单，设置初始时间戳
        if self.last_open_order_time == 0 and len(self.active_close_orders) > 0:
            self.last_open_order_time = time.time()

        # 检查是否已过冷却期
        if time.time() - self.last_open_order_time > cool_down_time:
            return 0  # 可以下单
        else:
            return 1  # 需要等待

    async def _place_and_monitor_open_order(self) -> bool:
        """下单并监控订单执行

        返回:
            bool - 是否成功处理订单（True: 成功, False: 失败）

        流程:
            1. 重置订单状态
            2. 下开仓订单
            3. 等待订单成交（最多10秒）
            4. 处理订单结果（成交或取消）
        """
        try:
            # 下单前重置状态
            self.order_filled_event.clear()  # 清除成交事件
            self.current_order_status = "OPEN"  # 设置订单状态为开仓
            self.order_filled_amount = 0.0  # 重置成交数量

            # 下开仓订单
            order_result = await self.exchange_client.place_open_order(
                self.config.contract_id, self.config.quantity, self.config.direction
            )

            # 如果下单失败，直接返回
            if not order_result.success:
                return False

            # 如果订单已经成交，立即处理
            if order_result.status == "FILLED":
                return await self._handle_order_result(order_result)

            # 否则等待订单成交（最多10秒）
            elif not self.order_filled_event.is_set():
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass  # 超时后继续处理

            # 处理订单结果
            return await self._handle_order_result(order_result)

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False

    async def _handle_order_result(self, order_result) -> bool:
        """处理订单执行结果

        参数:
            order_result: 订单结果对象，包含订单ID、价格、状态等信息

        返回:
            bool - 是否成功处理（True: 成功, False: 失败）

        处理逻辑:
            1. 如果订单完全成交 -> 下平仓订单
            2. 如果订单未成交 -> 取消订单
            3. 如果订单部分成交 -> 为成交部分下平仓订单
        """
        order_id = order_result.order_id
        filled_price = order_result.price

        # 情况1: 订单完全成交
        if self.order_filled_event.is_set() or order_result.status == "FILLED":
            if self.config.aster_boost:
                # Aster Boost模式: 使用市价单立即平仓（磨损更高但速度快）
                close_order_result = await self.exchange_client.place_market_order(
                    self.config.contract_id,
                    self.config.quantity,
                    self.config.close_order_side,
                )
            else:
                # 正常模式: 计算止盈价格并下限价平仓单
                self.last_open_order_time = time.time()
                close_side = self.config.close_order_side

                # 计算平仓价格
                if close_side == "sell":
                    # 做多平仓：在更高价格卖出
                    close_price = filled_price * (1 + self.config.take_profit / 100)
                else:
                    # 做空平仓：在更低价格买入
                    close_price = filled_price * (1 - self.config.take_profit / 100)

                close_order_result = await self.exchange_client.place_close_order(
                    self.config.contract_id,
                    self.config.quantity,
                    close_price,
                    close_side,
                )

                if not close_order_result.success:
                    self.logger.log(
                        f"[CLOSE] Failed to place close order: {close_order_result.error_message}",
                        "ERROR",
                    )
                    raise Exception(
                        f"[CLOSE] Failed to place close order: {close_order_result.error_message}"
                    )

                return True

        # 情况2: 订单未成交或超时
        else:
            self.order_canceled_event.clear()
            self.logger.log(
                f"[OPEN] [{order_id}] Cancelling order and placing a new order", "INFO"
            )

            # 尝试取消订单
            try:
                cancel_result = await self.exchange_client.cancel_order(order_id)
                if not cancel_result.success:
                    self.order_canceled_event.set()
                    self.logger.log(
                        f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}",
                        "ERROR",
                    )
                else:
                    self.current_order_status = "CANCELED"

            except Exception as e:
                self.order_canceled_event.set()
                self.logger.log(
                    f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR"
                )

            # 获取已成交数量
            if self.config.exchange == "backpack":
                # Backpack交易所直接从取消结果获取
                self.order_filled_amount = cancel_result.filled_size
            else:
                # 其他交易所等待取消事件或查询订单信息
                if not self.order_canceled_event.is_set():
                    try:
                        await asyncio.wait_for(
                            self.order_canceled_event.wait(), timeout=5
                        )
                    except asyncio.TimeoutError:
                        order_info = await self.exchange_client.get_order_info(order_id)
                        self.order_filled_amount = order_info.filled_size

            # 情况3: 如果有部分成交，为成交部分下平仓单
            if self.order_filled_amount > 0:
                close_side = self.config.close_order_side

                if self.config.aster_boost:
                    # Aster Boost模式：市价平仓
                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id, self.order_filled_amount, close_side
                    )
                else:
                    # 正常模式：限价平仓
                    if close_side == "sell":
                        close_price = filled_price * (1 + self.config.take_profit / 100)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit / 100)

                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.order_filled_amount,
                        close_price,
                        close_side,
                    )

                self.last_open_order_time = time.time()

                if not close_order_result.success:
                    self.logger.log(
                        f"[CLOSE] Failed to place close order: {close_order_result.error_message}",
                        "ERROR",
                    )

            return True

        return False

    async def _log_status_periodically(self):
        """定期记录状态信息，包括持仓和订单情况

        返回:
            bool - 是否检测到仓位不匹配（True: 检测到问题, False: 正常）

        功能:
            1. 每60秒记录一次状态
            2. 获取活跃订单和持仓信息
            3. 检查持仓与平仓订单是否匹配
            4. 如果发现不匹配，触发错误警报并关闭机器人
        """
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # 获取活跃订单
                active_orders = await self.exchange_client.get_active_orders(
                    self.config.contract_id
                )

                # 筛选出平仓订单
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append(
                            {
                                "id": order.order_id,
                                "price": order.price,
                                "size": order.size,
                            }
                        )

                # 获取当前持仓数量
                position_amt = await self.exchange_client.get_account_positions()

                # 计算活跃平仓订单的总数量
                active_close_amount = sum(
                    Decimal(order.get("size", 0))
                    for order in self.active_close_orders
                    if isinstance(order, dict)
                )

                self.logger.log(
                    f"Current Position: {position_amt} | Active closing amount: {active_close_amount} | "
                    f"Order quantity: {len(self.active_close_orders)}"
                )
                self.last_log_time = time.time()

                # 检查仓位不匹配问题
                # 理论上：持仓数量 = 平仓订单总数量（允许误差为2倍订单量）
                if abs(position_amt - active_close_amount) > (2 * self.config.quantity):
                    error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                    error_message += "Position mismatch detected\n"
                    error_message += (
                        "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    )
                    error_message += "Please manually rebalance your position and take-profit orders\n"
                    error_message += "请手动平衡当前仓位和正在关闭的仓位\n"
                    error_message += (
                        f"current position: {position_amt} | active closing amount: {active_close_amount} | "
                        f"Order quantity: {len(self.active_close_orders)}\n"
                    )
                    error_message += (
                        "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    )
                    self.logger.log(error_message, "ERROR")

                    # 发送飞书通知（如果配置了）
                    await self._lark_bot_notify(error_message.lstrip())

                    # 请求关闭机器人
                    if not self.shutdown_requested:
                        self.shutdown_requested = True

                    mismatch_detected = True
                else:
                    mismatch_detected = False

                return mismatch_detected

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            print("--------------------------------")

    async def _meet_grid_step_condition(self) -> bool:
        """检查是否满足网格步长条件

        返回:
            bool - 是否满足条件（True: 可以下单, False: 不满足条件需等待）

        功能:
            检查新订单的平仓价格与现有最近平仓订单价格之间的间距
            是否满足grid_step设置的最小百分比要求

        逻辑:
            - 如果没有活跃平仓订单，直接返回True
            - 做多(buy): 新平仓价必须比现有最低平仓价低至少grid_step%
            - 做空(sell): 新平仓价必须比现有最高平仓价高至少grid_step%
        """
        if self.active_close_orders:
            # 做多时选择价格最低的平仓订单，做空时选择价格最高的
            picker = min if self.config.direction == "buy" else max
            next_close_order = picker(
                self.active_close_orders, key=lambda o: o["price"]
            )
            next_close_price = next_close_order["price"]

            # 获取当前市场最优买卖价
            best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(
                self.config.contract_id
            )
            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                raise ValueError("No bid/ask data available")

            if self.config.direction == "buy":
                # 做多情况：计算新订单的平仓价格（卖出价）
                new_order_close_price = best_ask * (1 + self.config.take_profit / 100)
                # 检查: 现有平仓价 / 新平仓价 > 1 + grid_step%
                # 即: 新平仓价要比现有平仓价低至少grid_step%
                if (
                    next_close_price / new_order_close_price
                    > 1 + self.config.grid_step / 100
                ):
                    return True
                else:
                    return False
            elif self.config.direction == "sell":
                # 做空情况：计算新订单的平仓价格（买入价）
                new_order_close_price = best_bid * (1 - self.config.take_profit / 100)
                # 检查: 新平仓价 / 现有平仓价 > 1 + grid_step%
                # 即: 新平仓价要比现有平仓价高至少grid_step%
                if (
                    new_order_close_price / next_close_price
                    > 1 + self.config.grid_step / 100
                ):
                    return True
                else:
                    return False
            else:
                raise ValueError(f"Invalid direction: {self.config.direction}")
        else:
            # 没有活跃平仓订单，可以直接下单
            return True

    async def _check_price_condition(self) -> bool:
        """检查价格条件，判断是否需要停止或暂停交易

        返回:
            tuple[bool, bool] - (是否停止交易, 是否暂停交易)

        功能:
            根据配置的stop_price和pause_price参数，检查当前市场价格
            是否触发停止或暂停条件

        逻辑:
            做多(buy)方向:
                - stop_price: 当价格 >= stop_price时，停止交易并退出
                - pause_price: 当价格 >= pause_price时，暂停交易（等待价格回落）
            做空(sell)方向:
                - stop_price: 当价格 <= stop_price时，停止交易并退出
                - pause_price: 当价格 <= pause_price时，暂停交易（等待价格回升）
        """
        stop_trading = False
        pause_trading = False

        # 如果两个价格都未设置（都为-1），直接返回
        if self.config.pause_price == self.config.stop_price == -1:
            return stop_trading, pause_trading

        # 获取当前市场最优买卖价
        best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(
            self.config.contract_id
        )
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("No bid/ask data available")

        # 检查停止价格条件
        if self.config.stop_price != -1:
            if self.config.direction == "buy":
                # 做多：价格涨到stop_price以上，停止交易
                if best_ask >= self.config.stop_price:
                    stop_trading = True
            elif self.config.direction == "sell":
                # 做空：价格跌到stop_price以下，停止交易
                if best_bid <= self.config.stop_price:
                    stop_trading = True

        # 检查暂停价格条件
        if self.config.pause_price != -1:
            if self.config.direction == "buy":
                # 做多：价格涨到pause_price以上，暂停交易
                if best_ask >= self.config.pause_price:
                    pause_trading = True
            elif self.config.direction == "sell":
                # 做空：价格跌到pause_price以下，暂停交易
                if best_bid <= self.config.pause_price:
                    pause_trading = True

        return stop_trading, pause_trading

    async def _lark_bot_notify(self, message: str):
        """发送飞书机器人通知

        参数:
            message: 要发送的消息内容

        功能:
            如果配置了LARK_TOKEN环境变量，使用飞书机器人发送通知消息
            常用于发送错误警报或重要状态更新
        """
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as bot:
                await bot.send_text(message)

    async def run(self):
        """主交易循环 - 机器人的核心运行逻辑

        流程:
            1. 初始化：获取合约信息，连接交易所
            2. 主循环：
               a. 更新活跃订单列表
               b. 定期记录状态
               c. 检查价格条件（停止/暂停）
               d. 检查等待时间
               e. 检查网格步长条件
               f. 下单并监控
            3. 异常处理：键盘中断或其他错误时优雅关闭
        """
        try:
            # 获取合约属性（合约ID和最小价格变动单位）
            self.config.contract_id, self.config.tick_size = (
                await self.exchange_client.get_contract_attributes()
            )

            # 记录当前交易配置
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price}", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price}", "INFO")
            self.logger.log(f"Aster Boost: {self.config.aster_boost}", "INFO")
            self.logger.log("=============================", "INFO")

            # 捕获当前运行的事件循环，用于线程安全的回调
            self.loop = asyncio.get_running_loop()

            # 连接到交易所（建立WebSocket连接等）
            await self.exchange_client.connect()

            # 等待连接建立
            await asyncio.sleep(5)

            # 主交易循环
            while not self.shutdown_requested:
                # 1. 更新活跃订单列表
                active_orders = await self.exchange_client.get_active_orders(
                    self.config.contract_id
                )

                # 2. 筛选出平仓订单
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append(
                            {
                                "id": order.order_id,
                                "price": order.price,
                                "size": order.size,
                            }
                        )

                # 3. 定期记录状态（每60秒）
                mismatch_detected = await self._log_status_periodically()

                # 4. 检查价格条件
                stop_trading, pause_trading = await self._check_price_condition()
                if stop_trading:
                    # 触发停止价格，优雅关闭
                    msg = f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
                    msg += "Stopped trading due to stop price\n"
                    await self.graceful_shutdown(msg)
                    await self._lark_bot_notify(msg.lstrip())
                    continue

                if pause_trading:
                    # 触发暂停价格，等待5秒后重新检查
                    await asyncio.sleep(5)
                    continue

                # 5. 如果没有检测到仓位不匹配问题
                if not mismatch_detected:
                    # 计算等待时间
                    wait_time = self._calculate_wait_time()

                    if wait_time > 0:
                        # 需要等待，1秒后重新检查
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        # 检查网格步长条件
                        meet_grid_step_condition = (
                            await self._meet_grid_step_condition()
                        )
                        if not meet_grid_step_condition:
                            # 不满足网格步长条件，等待1秒
                            await asyncio.sleep(1)
                            continue

                        # 所有条件满足，下单并监控
                        await self._place_and_monitor_open_order()
                        self.last_close_orders += 1

        except KeyboardInterrupt:
            # 用户按Ctrl+C中断
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            # 其他严重错误
            self.logger.log(f"Critical error: {e}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # 确保所有连接都已关闭（即使优雅关闭失败）
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
