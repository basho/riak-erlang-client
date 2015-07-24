#pragma D option quiet
#pragma D option dynvarsize=128m

uint64_t encoding_start;
uint64_t meta_encoding_start;
/* First time we notice the PB process being scheduled. */
uint64_t pb_first_start;
/* Set when script has identified the PB server Erlang process */
int pb_proc_set;
/* Pid of the PB server Erlang process */
string pb_proc;

/* Mark first process that calls dyntrace:p(1,...) as the PB process we
   are looking at, remember its pid. */
erlang$target:::user_trace-i4s4
/!pb_proc_set /
{
    pb_proc = copyinstr(arg0);
    pb_proc_set = 1;
    printf("PB process id is %s\n", pb_proc);
}

/* Enter encoding section */
erlang$target:::user_trace-i4s4
/ arg2 == 1 && arg3 == 1/
{
    encoding_start = vtimestamp;
}

/* Leave encoding section */
erlang$target:::user_trace-i4s4
/ arg2 == 1 && arg3 == 2 && encoding_start /
{
    this->elapsed = vtimestamp - encoding_start;
    @encoding_time = sum(this->elapsed);
    @encoding_time_total = sum(this->elapsed);
    encoding_start = 0;
}

/* Enter meta encoding section */
erlang$target:::user_trace-i4s4
/ arg2 == 2  && arg3 == 1/
{
    meta_encoding_start = vtimestamp;
}

/* Leave meta encoding section */
erlang$target:::user_trace-i4s4
/ arg2 == 2 && arg3 == 2 && meta_encoding_start /
{
    this->elapsed = vtimestamp - meta_encoding_start;
    @meta_encoding_time = sum(this->elapsed);
    @meta_encoding_time_total = sum(this->elapsed);
    meta_encoding_start = 0;
}

/* First time we start measuring PB process time. */
erlang$target:::process-scheduled
/ copyinstr(arg0) == pb_proc && !pb_first_start /
{
    pb_first_start = timestamp;
}

/* Re-scheduled during meta encoding. */
erlang$target:::process-scheduled
/ copyinstr(arg0) == pb_proc && encoding_start /
{
    encoding_start = vtimestamp;
}

/* Re-scheduled during meta encoding. */
erlang$target:::process-scheduled
/ copyinstr(arg0) == pb_proc && meta_encoding_start /
{
    meta_encoding_start = vtimestamp;
}

erlang$target:::process-scheduled
/ copyinstr(arg0) == pb_proc /
{
    self->pb_start = timestamp;
    self->pb_vstart = vtimestamp;
    @pb_scheduled = count();
}

/* De-scheduled during encoding. Remember encoding time so far. */
erlang$target:::process-unscheduled
/ self->pb_start && encoding_start /
{
    this->elapsed = vtimestamp - encoding_start;
    @encoding_time = sum(this->elapsed);
    @encoding_time_total = sum(this->elapsed);
}

/* De-scheduled during meta encoding. Remember encoding time so far. */
erlang$target:::process-unscheduled
/ self->pb_start && meta_encoding_start /
{
    this->elapsed = vtimestamp - meta_encoding_start;
    @meta_encoding_time = sum(this->elapsed);
    @meta_encoding_time_total = sum(this->elapsed);
}

erlang$target:::process-unscheduled
/ self->pb_start /
{
    this->elapsed = timestamp - self->pb_start;
    this->velapsed = vtimestamp - self->pb_vstart;
    @proc_time = sum(this->elapsed);
    @proc_vtime = sum(this->velapsed);
    @proc_time_total = sum(this->elapsed);
    @proc_vtime_total = sum(this->velapsed);
    self->pb_start = 0;
}

/* ============================= Measure garbage collection ================*/
/* Store starting time in thread for GCs */
erlang$target:::gc_minor-start,
erlang$target:::gc_major-start
/ copyinstr(arg0) == pb_proc /
{
    self->gc_start = vtimestamp;
}

/* Total GC time on behalf of PB process. */
erlang$target:::gc_minor-end,
erlang$target:::gc_major-end
/ self->gc_start /
{
    this->elapsed = vtimestamp - self->gc_start;
    @gc_time = sum(this->elapsed);
    @gc_time_total = sum(this->elapsed);
    self->gc_start = 0;
}

profile:::tick-1s
{
    printf("===================================================\n");
    printa("PB proc scheduled %@u times\n", @pb_scheduled);
    clear(@pb_scheduled);

    normalize(@proc_time, 1000000);
    printa("PB proc time : %@ums\n", @proc_time);
    clear(@proc_time);
    
    normalize(@proc_vtime, 1000000);
    printa("PB proc vtime : %@ums\n", @proc_vtime);
    clear(@proc_vtime);
    
    normalize(@gc_time, 1000000);
    printa("GC time : %@ums\n", @gc_time);
    clear(@gc_time);

    normalize(@encoding_time, 1000000);
    normalize(@meta_encoding_time, 1000000);
    printa("Meta Encoding Time : %@10ums\n", @meta_encoding_time);
    printa("Encoding Time : %@10ums\n", @encoding_time);
    clear(@meta_encoding_time);
    clear(@encoding_time);

    normalize(@proc_time_total, 1000000);
    normalize(@proc_vtime_total, 1000000);
    normalize(@encoding_time_total, 1000000);
    normalize(@meta_encoding_time_total, 1000000);
    normalize(@gc_time_total, 1000000);

    self->total_time = pb_first_start? timestamp - pb_first_start : 0;
    printf("Total time : %ums\n", self->total_time / 1000000);
    printa("Total PB proc time : %@ums\n", @proc_time_total);
    printa("Total PB proc vtime : %@ums\n", @proc_vtime_total);
    printa("Total encoding time: %@10ums\n", @encoding_time_total);
    printa("Total meta encoding time: %@10ums\n", @meta_encoding_time_total);
    printa("Total GC time: %@ums\n", @gc_time_total);
}
