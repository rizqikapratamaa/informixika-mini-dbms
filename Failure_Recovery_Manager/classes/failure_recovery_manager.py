from .buffer import buffer, table
from .recovery import recover
from Query_Processor.classes.execution_result import ExecutionResult
from .logclass import RecoveryCriteria, actiontype
from .logger import create_log
from .log_rw import write_log as wl
from .update_finder import apply_update, changeReport
from datetime import datetime
import threading
import time
from Utils.component_logger import log_frm


class failure_recovery_manager:
    initiated = False

    def __init__(self, checkpoint_clock: bool = True):
        #
        self.lock = threading.Lock()
        self.buf: buffer = buffer()
        self.checkpoint_routine = None

        if checkpoint_clock:
            self.running = True
            self.timer_interval = 5*60  # 5*60
            self.time_to_check = 1 * self.timer_interval
            self.timer_thread = threading.Thread(target=self.timer_task, daemon=True)
            self.timer_thread.start()
        else:
            self.running = False

    def write_log_stamp(self, transaction_id: int, action: actiontype):
        with self.lock:
            wl(create_log(transaction_id, action))

    # Harus dites
    def write_log(self, result: ExecutionResult):
        with self.lock:
            action: str = result.query.split(" ")[0].lower()
            tablename: str = result.query.split(" ")[1]
            assert (
                tablename != None
                and tablename != ""
                and action.lower() in ["update", "insert", "delete"]
            )
            if action == "update":
                er: changeReport = apply_update(
                    result, tablename, self.buf, actiontype.write, update=True
                )
            elif action == "insert":
                er: changeReport = apply_update(
                    result, tablename, self.buf, actiontype.write, insert=True
                )
            elif action == "delete":
                er: changeReport = apply_update(
                    result, tablename, self.buf, actiontype.write, delete=True
                )

            old_data = er.old_data
            new_data = er.new_data

            # anggep selalu semacem write deh biar gampang
            # disini panggil sesuatu dari update_finder
            wl(
                create_log(
                    result.transaction_id,
                    actiontype.write,
                    old_data,
                    new_data,
                    tablename,
                )
            )

    # Harus dites
    def recover(self, criteria: RecoveryCriteria = None):
        with self.lock:
            if criteria.transaction_id != None:
                recover(transaction_id=criteria.transaction_id, buf=self.buf)
            else:
                recover(timestamp=criteria.timestamp, buf=self.buf)

    def rollback(self, timestamp=None, checkpoint=None):
        pass

    def debug_id(self):
        print("Yes")

    # bisa dipastikan hanya minta satu tabel at a time
    def table_from_buffer(self, table_name: str) -> table | list[dict] | None:
        with self.lock:
            log_frm("Buffering")
            retval: table = self.buf.get_table(table_name)
            if retval != None:
                return retval.data
            else:
                return None
            return self.buf.get_table(table_name)

    def send_table_to_buffer(self, table_name: str, data: list[dict]):
        with self.lock:
            return self.buf.insert_table(table(table_name, data))

    # Belum implement 100%
    def _save_checkpoint(self):
        self.checkpoint_routine()
        log_frm("Buffer writen")

        # for table in self.buf.data:
        #    #storage_manager.instance.write_to_disk(table_name : str, data : list[dict])
        #    pass
        #
        # on success

        # log checkpoint, or in this case, change files lmao

        return True

    def timer_task(self):
        ## Checks every few minutes if buffer should be checkpointed or no
        interval: int = 1
        ###print("[FRM | Checkpoint] Timer initiated")
        while self.running:
            if self.time_to_check > 0 and self.running:
                self.time_to_check -= interval
                time.sleep(interval)  # Wait for the timer interval
                # print("[FRM | Checkpoint] Tick")
            elif self.running:
                log_frm("Checking if checkpoint is needed")
                with self.lock:  # Block other threads during this operation
                    # If should checkpoint
                    if self.buf._should_save_checkpoint():
                        self._save_checkpoint()
                        log_frm("Done!")
                        self.time_to_check = 1 * self.timer_interval
                self.time_to_check = 1 * self.timer_interval
            else:
                break
        log_frm("Thread dying")

    def exit_routine(self):
        log_frm("Beginning exit routine")
        if self.running:
            self.running = False
            self.time_to_check = 0
            self.timer_thread.join()
        self._save_checkpoint()
        pass


"""    
    def __del__(self):
        self.exit_routine()
        pass
"""


class Failure_recovery_manager(failure_recovery_manager):
    _instance: failure_recovery_manager = None

    def enable(checkpoint_timer: bool = True) -> failure_recovery_manager:
        global _instance
        if not failure_recovery_manager.initiated:
            _instance = failure_recovery_manager(checkpoint_clock=checkpoint_timer)
            failure_recovery_manager.initiated = True
        else:
            log_frm("Failure recovery manager is already active")
        return _instance

    def set_checkpoint_routine(func: callable):
        global _instance
        _instance.checkpoint_routine = func

    def __init__(self):
        super()
        failure_recovery_manager.instance = self

