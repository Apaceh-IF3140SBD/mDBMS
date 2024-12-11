from CCWrapper import ConcurrencyControlWrapper
from Utils import Action, Row

# Initialize the Concurrency Control Wrapper
# cc_wrapper = ConcurrencyControlWrapper(algorithm='Timestamp')
cc_wrapper = ConcurrencyControlWrapper(algorithm='TwoPhaseLocking')


def test_complex_timestamp_based_cc():
    print("\nTesting Complex Timestamp-based Concurrency Control\n")

    tid1 = cc_wrapper.begin_transaction()

    row1 = Row(data=[(1, "Alice", 30, 100000.0),
                     (2, "Bob", 25, 75000.0),
                     (3, "Charlie", 28, 80000.0)], rows_count=3)
    row2 = Row(data=[(4, "Diana", 35, 95000.0),
                     (5, "Eve", 40, 120000.0)], rows_count=2)
    row3 = Row(data=[(6, "Frank", 45, 130000.0),
                     (7, "Grace", 50, 140000.0)], rows_count=2)

    cc_wrapper.log_object(row1, tid1)
    response1 = cc_wrapper.validate_object(row1, tid1, Action.READ)
    print(f"Transaction {tid1} READ on Row1: {response1}")

    tid2 = cc_wrapper.begin_transaction()

    cc_wrapper.log_object(row2, tid2)
    response2 = cc_wrapper.validate_object(row2, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row2 (Record 4): {response2}")

    response3 = cc_wrapper.validate_object(row1, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row1 (Record 2): {response3}")

    tid3 = cc_wrapper.begin_transaction()
    cc_wrapper.log_object(row3, tid3)
    response4 = cc_wrapper.validate_object(row3, tid3, Action.READ)
    print(f"Transaction {tid3} READ on Row3: {response4}")

    response5 = cc_wrapper.validate_object(row1, tid3, Action.WRITE)
    print(f"Transaction {tid3} WRITE on Row1 (Record 1): {response5}")

    response6 = cc_wrapper.validate_object(row1, tid1, Action.WRITE)
    print(f"Transaction {tid1} WRITE on Row1 (Record 2): {response6}")

    cc_wrapper.end_transaction(tid1)

    response7 = cc_wrapper.validate_object(row2, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row2 (Record 4): {response7}")

    cc_wrapper.end_transaction(tid2)

    cc_wrapper.end_transaction(tid3)



def test_twoPL_cc():
    print("\nTesting Two-Phase Locking Concurrency Control\n")
    
    tid1 = cc_wrapper.begin_transaction()

    row1 = Row(data=[(1, "Alice", 30, 100000.0),
                     (2, "Bob", 25, 75000.0),
                     (3, "Charlie", 28, 80000.0)], rows_count=3)
    row2 = Row(data=[(4, "Diana", 35, 95000.0),
                     (5, "Eve", 40, 120000.0)], rows_count=2)
    row3 = Row(data=[(6, "Frank", 45, 130000.0),
                     (7, "Grace", 50, 140000.0)], rows_count=2)

    cc_wrapper.log_object(row1, tid1)
    response1 = cc_wrapper.validate_object(row1, tid1, Action.READ)
    print(f"Transaction {tid1} READ on Row1: {response1}")

    tid2 = cc_wrapper.begin_transaction()
    response2 = cc_wrapper.validate_object(row1, tid2, Action.READ)
    print(f"Transaction {tid2} READ on Row1: {response2}")  

    # Test Case 3: Exclusive lock conflict
    response3 = cc_wrapper.validate_object(row1, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row1: {response3}") 

    # Test Case 4: Independent operations on different rows
    cc_wrapper.log_object(row2, tid2)
    response4 = cc_wrapper.validate_object(row2, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row2: {response4}")  

    # Test Case 5: Lock upgrade attempt
    tid3 = cc_wrapper.begin_transaction()
    response5 = cc_wrapper.validate_object(row2, tid3, Action.READ)
    print(f"Transaction {tid3} READ on Row2: {response5}")  

    cc_wrapper.end_transaction(tid1)
    
    response6 = cc_wrapper.validate_object(row1, tid3, Action.WRITE)
    print(f"Transaction {tid3} WRITE on Row1: {response6}")  

    cc_wrapper.end_transaction(tid2)
    cc_wrapper.end_transaction(tid3)


def test_twoPL_abort():
    print("\nTesting Two-Phase Locking Abort Scenarios\n")
    
    cc_wrapper = ConcurrencyControlWrapper(algorithm='TwoPhaseLocking')

    row1 = Row(data=[(1, "Alice", 30, 100000.0),
                     (2, "Bob", 25, 75000.0)], rows_count=2)
    row2 = Row(data=[(3, "Charlie", 35, 95000.0),
                     (4, "Diana", 40, 120000.0)], rows_count=2)

    tid1 = cc_wrapper.begin_transaction()
    
    cc_wrapper.log_object(row1, tid1)
    response1 = cc_wrapper.validate_object(row1, tid1, Action.READ)
    print(f"Transaction {tid1} READ on Row1: {response1}")
    
    tid2 = cc_wrapper.begin_transaction()
    
    cc_wrapper.log_object(row2, tid2)
    response2 = cc_wrapper.validate_object(row2, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row2: {response2}")
    
    cc_wrapper.end_transaction(tid1)
    
    response3 = cc_wrapper.validate_object(row2, tid1, Action.READ)
    print(f"Transaction {tid1} READ on Row2: {response3}")  # Should abort
    
    cc_wrapper.end_transaction(tid2)


def test_twoPL_deadlock():
    print("\nTesting Two-Phase Locking Deadlock Scenario\n")
    
    cc_wrapper = ConcurrencyControlWrapper(algorithm='TwoPhaseLocking')

    row1 = Row(data=[(1, "Alice", 30, 100000.0)], rows_count=1)
    row2 = Row(data=[(2, "Bob", 25, 75000.0)], rows_count=1)

    tid1 = cc_wrapper.begin_transaction()
    
    cc_wrapper.log_object(row1, tid1)
    response1 = cc_wrapper.validate_object(row1, tid1, Action.READ)
    print(f"Transaction {tid1} READ on Row1: {response1}")
    
    tid2 = cc_wrapper.begin_transaction()
    
    cc_wrapper.log_object(row2, tid2)
    response2 = cc_wrapper.validate_object(row2, tid2, Action.READ)
    print(f"Transaction {tid2} READ on Row2: {response2}")

    response3 = cc_wrapper.validate_object(row2, tid1, Action.WRITE)
    print(f"Transaction {tid1} WRITE on Row2: {response3}")  
    
    response4 = cc_wrapper.validate_object(row1, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row1: {response4}")  

    response5 = cc_wrapper.validate_object(row1, tid1, Action.WRITE)
    print(f"Transaction {tid1} WRITE on Row1: {response5}")  

    response6 = cc_wrapper.validate_object(row1, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row1: {response6}")  
    
    # Clean up
    cc_wrapper.end_transaction(tid1)
    cc_wrapper.end_transaction(tid2)


def test_twoPL_deadlock_abort():
    print("\nTesting Two-Phase Locking Deadlock with Abort Resolution\n")
    
    cc_wrapper = ConcurrencyControlWrapper(algorithm='TwoPhaseLocking')

    row1 = Row(data=[(1, "Alice", 30, 100000.0)], rows_count=1)
    row2 = Row(data=[(2, "Bob", 25, 75000.0)], rows_count=1)

    tid1 = cc_wrapper.begin_transaction()
    cc_wrapper.log_object(row1, tid1)
    response1 = cc_wrapper.validate_object(row1, tid1, Action.WRITE)
    print(f"Transaction {tid1} WRITE on Row1: {response1}")  
    
    tid2 = cc_wrapper.begin_transaction()
    cc_wrapper.log_object(row2, tid2)
    response2 = cc_wrapper.validate_object(row2, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row2: {response2}")  

    cc_wrapper.log_object(row2, tid1)
    response3 = cc_wrapper.validate_object(row2, tid1, Action.WRITE)
    print(f"Transaction {tid1} WRITE on Row2: {response3}")  

    cc_wrapper.log_object(row1, tid2)
    response4 = cc_wrapper.validate_object(row1, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row1: {response4}")  

    response5 = cc_wrapper.validate_object(row2, tid2, Action.WRITE)
    print(f"Transaction {tid2} WRITE on Row2: {response5}") 

    cc_wrapper.end_transaction(tid1)
    cc_wrapper.end_transaction(tid2)


# test_twoPL_deadlock()
# test_complex_timestamp_based_cc()
test_twoPL_cc()
# test_twoPL_abort()
# test_twoPL_deadlock_abort()