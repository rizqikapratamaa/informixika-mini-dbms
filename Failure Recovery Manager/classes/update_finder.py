#digunakan untuk membandingkan 2 tabel, apa yang baru, apa yang berubah, dan apa yang dihapus
from Storage_Manager.classes.rows import Rows
from Query_Processor.classes.execution_result import ExecutionResult
from .buffer import buffer, table
from .logclass import actiontype as AT
from Utils.component_logger import log_frm

# Returns how many keys are the same
def key_compare(dataA : dict, dataB : dict) -> int :
    if (len(dataA.keys) != len(dataB.keys)):
        print("[FRM|KeyCompare] Dictionary key size mismatch!")
        return -1
    for i in range(len(dataA.keys)):
        if (dataA.keys[i] != dataB.keys[i]):
            print("[FRM|KeyCompare] Dictionary keys equality mismatch!")
            return -1
    
    retval : int = 0
    for key in dataA.keys:
        if (dataA[key] == dataB[key]):
            retval += 1
    return retval
    
def determine_index(main_table : table, data : Rows) -> list[int]:
    # Menerima main table dan data, lalu menentukan sebaiknya "indexing" pake kolom yang mana
    # harus menggunakan buffer dari SM
    # nantinya harus handle update index
    # debuffer ada ga indexnya? kalau ada kita pake determine_index kalau ga ada tentuin sendiri
    if (data.idx != None):
        return data.idx
    else:
        log_frm("INDEX IS NOT SUPPLIED, Beginning determine_index")
        if not main_table or not data:
            raise ValueError(f"[FRM] Main table (typeof {type(main_table)}) cannot be empty.")
        if not data:
            raise ValueError(f"[FRM] Data cannot be empty.")

        columns = main_table.data[0].keys()

        column_scores = {}
        
        #Check for dupes, if a column has dupes, avoid using it as index
        #Should there be no columns that satisfy a 100% rate, attempt pairing 2 or more of the highest
        #Scoring values
        
        for column in columns:
            val : list[int] = []
            # Kumpulkan nilai unik untuk setiap kolom di main_table dan data
            main_table_values = {row[column] for row in main_table.data if column in row}
            data_values = {row[column] for row in data if column in row}

            # Jika nilai selalu ada di kedua tabel dan dapat membentuk pasangan unik
            j = 0 # Loop index
            for i in range(len(data_values)):
                item = data_values[i]
                if j < len(main_table_values):
                    while main_table_values[j] != item and j < len(main_table_values):
                        j += 1
                if item[i] == main_table_values[j]:
                    column_scores[column] = j
                    j += 1
                else:
                    val = [] #cannot use collumn for index
                
                    
            column_scores[column] = val

        best_index = max(column_scores, key= lambda k : len(column_scores[k]) )
        
        if len(best_index) < len(data):
            log_frm("[FRM] WARNING ! Couldn't find an index that suites all data!")
        
        if len(best_index) == 0:
            raise ValueError("Tidak ditemukan kolom yang dapat digunakan sebagai indeks unik.")

        return best_index


# BTW ini bakal ngeoverwrite, semua nya itu reference bagi dia, jadi hati2
class changeReport:
    def __init__(self,olddata : list[dict] = [], newdata : list[dict] = []):
        self.old_data = olddata
        self.new_data = newdata
    
    # buffer akan menjadi buffer
def apply_update(er : ExecutionResult, tablename : str, buffer : buffer, action : AT, insert : bool = False, delete : bool = False, update : bool = False) -> changeReport :
    assert((insert and not delete and not update) | (not insert and delete and not update) or (not insert and not delete and update))
    index : str = determine_index(buffer.get_table(tablename), er.data) ######
    report : changeReport = changeReport([],[])
    cur_table = buffer.tables[buffer._table_id(tablename)]
    assert(len(index) == len(er.data.data))
    prog = 0
    for item in er.data.data:
        # Expensive check all method
        if (not insert):
                if (delete):
                    cur_table.data[index[prog]] = {}
                    report.old_data += [item]
                elif (update):
                    report.new_data += [item]
                    report.old_data += [cur_table.data[index[prog]]]
                    cur_table.data[index[prog]] = item
        else :
            buffer.tables += [item]
            report.new_data += [item]
        prog += 1

    # harus update index

    return report