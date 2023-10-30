import pyodbc
import pandas as pd


class Delete_PO:
    global PO_ID,conn
    PO_ID = 'PO207345'

    conn = ("""driver={SQL Server};server={{SQL Host}};database={{Database Name}};
        trusted_connection=no;UID={{DB_UserID}};PWD={{DB_password}};IntegratedSecurity = true;""")
    conx = pyodbc.connect(conn)
    cur = conx.cursor()
    
    def ArtsanaPoBatchLines(self):             # 1 = ArtsanaPoBatchLines

        sql_query1 = pd.read_sql_query("SELECT * FROM ArtsanaPoBatchLines WHERE POLinesRecNumber IN (SELECT	RECORD_NOFROMPURCHASE_ORDER_LINESWHEREPO_ID ='"+PO_ID+"')",self.conx)
        df = pd.DataFrame(sql_query1)
        df.to_csv(r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_PO\Backup_ArtsanaPoBatchLines.csv' , index= False)
        print("ArtsanaPoBatchLines export complete")   

        Delete_query1 = "DELETE FROM ArtsanaPoBatchLines WHERE POLinesRecNumber IN (SELECT RECORD_NOFROMPURCHASE_ORDER_LINESWHEREPO_ID ='"+PO_ID+"')"
        exe1 = self.cur.execute(Delete_query1)
        self.cur.commit()
        print("ArtsanaPoBatchLines Deleted Successfully",PO_ID)


    def PurchaseOrderReferences(self):            # 2 = PurchaseOrderReferences

        sql_query2 = pd.read_sql_query("SELECT * FROM PurchaseOrderReferences WHERE PO_ID ='"+PO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query2)
        df.to_csv(r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_PO\Backup_PurchaseOrderReferences.csv' , index= False)
        print("PurchaseOrderReferences export complete")   

        Delete_query2 = "DELETE FROM PurchaseOrderReferences WHERE PO_ID = = '"+PO_ID+"'"
        exe2 = self.cur.execute(Delete_query2)
        self.cur.commit()
        print("PurchaseOrderReferences Deleted Successfully",PO_ID)


    def PURCHASE_ORDER_LINES(self):            # 3 = PURCHASE_ORDER_LINES DETAIL

        sql_query3 = pd.read_sql_query("SELECT * FROM PURCHASE_ORDER_LINES WHERE PO_ID = '"+PO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query3)
        df.to_csv(r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_PO\Backup_PURCHASE_ORDER_LINES.csv' , index= False)
        print("PURCHASE_ORDER_LINES DETAIL export complete")   

        Delete_query3 = "DELETE FROM PURCHASE_ORDER_LINES WHERE PO_ID = '"+PO_ID+"'"
        exe3 = self.cur.execute(Delete_query3)
        self.cur.commit()
        print("PURCHASE_ORDER_LINES DETAIL Deleted Successfully",PO_ID)


    def PURCHASE_ORDER(self):             # 4 = PURCHASE_ORDER

        sql_query4 = pd.read_sql_query("SELECT * FROM PURCHASE_ORDER WHERE PO_ID = '"+PO_ID+"'",self.conx)
        df = pd.DataFrame(sql_query4)
        df.to_csv(r'C:\Users\Emiza\Desktop\BACKUP_ARTSANA_PO\Backup_ARTSANA_OUTBOUND_DELIVERY_SALES ORDER NOTE.csv' , index= False)
        print("PURCHASE_ORDER ORDER NOTE export complete")   

        Delete_query4 = "DELETE FROM PURCHASE_ORDER WHERE DELIVERY = '"+PO_ID+"'"
        exe4 = self.cur.execute(Delete_query4)
        self.cur.commit()
        print("PURCHASE_ORDER Deleted Successfully",PO_ID)

a1 = Delete_PO()
a1.ArtsanaPoBatchLines()
a1.PurchaseOrderReferences()
a1.PURCHASE_ORDER_LINES()
a1.PURCHASE_ORDER()
