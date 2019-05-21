

package com.couchbase.test.transactions;

import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;


import reactor.util.function.Tuple2;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.TransactionJsonDocument;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.Transactions;

public class SimpleTransaction {


	public Transactions createTansaction(Cluster cluster, TransactionConfig config) {
		return Transactions.create(cluster, config);
	}
	
	public TransactionConfig createTransactionConfig(int expiryTimeout, int changedurability) {
		TransactionConfigBuilder config = TransactionConfigBuilder.create();
		if (changedurability > 0) {
			switch (changedurability) {
	        case 1:
	            config.durabilityLevel(TransactionDurabilityLevel.MAJORITY);
	            break;
	        case 2:
	            config.durabilityLevel(TransactionDurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER);
	            break;
	        case 3:
	            config.durabilityLevel(TransactionDurabilityLevel.PERSIST_TO_MAJORITY);
	            break;
	        case 4:
	            config.durabilityLevel(TransactionDurabilityLevel.NONE);
	            break;
	        default:
	        	config.durabilityLevel(TransactionDurabilityLevel.NONE);
			}
		}
		
		return config.expirationTime(Duration.of(expiryTimeout, ChronoUnit.SECONDS)).build();
	}
	
	public ArrayList<LogDefer> CreateTransaction(Transactions transaction, List<Collection> collections, List<Tuple2<String, Object>> Createkeys, Boolean commit) {
		ArrayList<LogDefer> res = null;
		try {
			transaction.run(ctx -> {
				//				creation of docs
								for (Collection bucket:collections) {
									for (Tuple2<String, Object> document : Createkeys) {
										TransactionJsonDocument doc=ctx.insert(bucket, document.getT1(), document.getT2()); 
	//									TransactionJsonDocument doc1=ctx.get(bucket, document.getT1()).get();
	//									if (doc1 != doc) { System.out.println("Document not matched");	}
									}
								}
		});}
		catch (TransactionFailed err) {
            // This per-txn log allows the app to only log failures
			System.out.println("Transaction failed from runTransaction");
//			err.result().log().logs().forEach(System.err::println);
            res = err.result().log().logs();
        }
		return res;
	}
	
	public ArrayList<LogDefer> UpdateTransaction(Transactions transaction, List<Collection> collections, List<String> Updatekeys, Boolean commit, int updatecount) {
		ArrayList<LogDefer> res = null;
		try {
			transaction.run(ctx -> {
				for (String key: Updatekeys) {
					for (Collection bucket:collections) {
						if (ctx.get(bucket, key).isPresent()) {
							TransactionJsonDocument doc2=ctx.get(bucket, key).get();
							for (int i=1; i<=updatecount; i++) {
									JsonObject content = doc2.contentAs(JsonObject.class);
									content.put("mutated", i );	
									ctx.replace(doc2, content);
								}
							}
							
					}
				}
		});}
		catch (TransactionFailed err) {
            // This per-txn log allows the app to only log failures
			System.out.println("Transaction failed from runTransaction");
//			err.result().log().logs().forEach(System.err::println);
            res = err.result().log().logs();
        }
		return res;
	}
	
	public ArrayList<LogDefer> DeleteTransaction(Transactions transaction, List<Collection> collections, List<String> Deletekeys, Boolean commit) {
		ArrayList<LogDefer> res = null;
		try {
			transaction.run(ctx -> {
//				   delete the docs
						for (String key: Deletekeys) {
							for (Collection bucket:collections) {
								if (ctx.get(bucket, key).isPresent()) {
									TransactionJsonDocument doc1=ctx.get(bucket, key).get();
									ctx.remove(doc1);
								}
							}
						}
		});}
		catch (TransactionFailed err) {
            // This per-txn log allows the app to only log failures
			System.out.println("Transaction failed from runTransaction");
//			err.result().log().logs().forEach(System.err::println);
            res = err.result().log().logs();
        }
		return res;
	}
	
	public ArrayList<LogDefer> RunTransaction(Transactions transaction, List<Collection> collections, List<Tuple2<String, Object>> Createkeys, List<String> Updatekeys, 
			List<String> Deletekeys, Boolean commit, boolean sync, int updatecount) {
		ArrayList<LogDefer> res = null;
//		synchronous API - transactions
		if (sync) {
			try {
				TransactionResult result = transaction.run(ctx -> {
	//				creation of docs
					for (Collection bucket:collections) {
						for (Tuple2<String, Object> document : Createkeys) {
							TransactionJsonDocument doc=ctx.insert(bucket, document.getT1(), document.getT2()); 
//							TransactionJsonDocument doc1=ctx.get(bucket, document.getT1()).get();
//							if (doc1 != doc) { System.out.println("Document not matched");	}
						}

					}
	//				update of docs
					for (String key: Updatekeys) {
						for (Collection bucket:collections) {
							if (ctx.get(bucket, key).isPresent()) {
								TransactionJsonDocument doc2=ctx.get(bucket, key).get();
								for (int i=1; i<=updatecount; i++) {
										JsonObject content = doc2.contentAs(JsonObject.class);
										content.put("mutated", i );	
										ctx.replace(doc2, content);
									}
								}
								
						}
					}
	//			   delete the docs
					for (String key: Deletekeys) {
						for (Collection bucket:collections) {
							if (ctx.get(bucket, key).isPresent()) {
								TransactionJsonDocument doc1=ctx.get(bucket, key).get();
								ctx.remove(doc1);
							}
						}
					}
	//				commit ot rollback the docs
					if (commit) {  ctx.commit(); }
					else { ctx.rollback(); 	 }
					
//					transaction.close();
					
					
				});
				result.log().logs().forEach(System.err::println);
				
				}
			catch (TransactionFailed err) {
	            // This per-txn log allows the app to only log failures
				System.out.println("Transaction failed from runTransaction");
				err.result().log().logs().forEach(System.err::println);
	            res = err.result().log().logs();
	        }
			
		}
//		Asynchronous transactions API 
		else {
			System.out.println("IN ELSE PART");

		}
		return res;
		}
	
	public void nonTxnRemoves(Collection collection) {
	    String id = "collection_0";
	    JsonObject initial = JsonObject.create().put("val", 1);
	 
	    for (int i = 0; i < 100000; i ++) {
	        collection.insert(id, initial, InsertOptions.insertOptions().withDurabilityLevel(DurabilityLevel.MAJORITY));
	        collection.remove(id, RemoveOptions.removeOptions().withDurabilityLevel(DurabilityLevel.MAJORITY));
	    }
	}
	
}


