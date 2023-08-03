{% docs stop_statuses %}

An enumeration of valid statuses a stop can have. Used for the stop table.

- Arrived - truck is currently at the stop location, but not in the building yet
- Completed - stop is completed
- Front Office Cancelled - customer service cancelled this stop due to customer request
- Front Office Merged - customer service merged this stop into another stop for expediency
- Front Office Rescheduled - customer service rescheduled this stop
- Left Truck - \[Deprecated\]
- Not Started - stop is scheduled, but no actions to arriving at that stop have started yet. Otherwise, we would have 'On The Way'
- On Premise - team is within the building at the stop
- On The Way - team is currently headed to that stop
- Truck Cancelled - field team requested a cancellation for this stop
- Truck Rescheduled - \[Deprecated\]
- Waiting for customer - truck is at stop, but customer isn't ready.


{% enddocs %}