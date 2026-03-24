package util

type Result interface { // implements ListenableFuture<Value> {
	Get() interface{}
	IsSuccess() bool
}

type ResultStatus uint

const (
	PENDING = ResultStatus(iota)
	SUCCESS
	FAILED
	CANCELLED
)

type ResultStatusReason uint

const (
	NONE = ResultStatusReason(iota)
	ENTITY_NOT_FOUND
	DUPLICATE_ENTITIES_FOUND
	PERSIST_FAILED
	UNSUPPORTED_MESSAGE_VERSION
	NO_DEPENDENCY_MANAGER
	NO_RESOURCE_MANAGER
	NO_ENTITY_MANAGER
	CALCULATION_FAILED
	GET_FAILED
	EXECUTION_EXCEPTION
	UNEXPECTED
	ENITIY_EXISTS
	UNSUPPORTED_OPERATION
	CONFIGURATION_EXCEPTION
	RESOLVER_NOT_FOUND
	UNKNOWN_PREVIOUS_REQUEST
	DUPLICATE_REQUEST_FOUND
)

type result struct {
	resultChannel chan interface{}
	result        interface{}
	status        ResultStatus
	reason        ResultStatusReason
	statusInfo    string
	failureCause  error
}

func NewResult() Result {
	return &result{}
}

/*
	protected static final Object NULL_RESULT=new Object();

	public Result( Value result ) {
		this(ResultStatus.SUCCESS,null,null,null);
		set(result);
	}

	public Result() {
		this(SettableFuture.<Value>create(),ResultStatus.SUCCESS,null,null,null);
	}

	public Result( ListenableFuture<Value> result ) {
		this(result,ResultStatus.SUCCESS,null,null,null);
	}

	public Result(Throwable error) {
		this(ResultStatus.FAILED,ResultStatusReason.EXECUTION_EXCEPTION,error);
	}

	public Result( ResultStatus status, ResultStatusReason reason ) {
		this(null,status,reason,null,null);
	}

	public Result( ResultStatus status, ResultStatusReason reason, String statusInfo ) {
		this(status,reason,statusInfo, null);
	}

	public Result( ResultStatus status, ResultStatusReason reason, String statusInfo, Throwable cause ) {
		this(null,status,reason, statusInfo,cause);
	}

	public Result( ResultStatus status, ResultStatusReason reason, Throwable cause ) {
		this(null,status,reason,cause.getMessage(), cause);
	}

	protected Result( Result<Value> other ) {
		this( other.result, other.status, other.reason, other.statusInfo , other.failureCause );
	}

	protected Result(ListenableFuture<Value> result, ResultStatus status, ResultStatusReason reason, String statusInfo, Throwable cause ) {
		super();
		this.result = result;
		this.status = status;
		this.reason = reason;
		this.statusInfo = statusInfo;
		this.failureCause=cause;
	}

	public boolean isFailure() {
		return getStatus()==ResultStatus.FAILED;
	}
*/

func (result *result) IsSuccess() bool {
	return result.Status() == SUCCESS
}

/*
	public boolean hasStatusInfo() {
		getStatus();
		return statusInfo!=null;
	}

	public String getStatusInfo() {
		getStatus();
		return statusInfo;
	}
*/
func (result *result) Status() ResultStatus {
	result.Get()
	return result.status
}

/*
	public void set(Value result) {
		if (!(this.result instanceof SettableFuture))
			this.result=SettableFuture.<Value>create();
		((SettableFuture<Value>)this.result).set(result);
	}

	public void setException(Throwable exception ) {
		set(new Result<Value>(exception) );
	}

	protected void set( Result<Value> other)  {
		this.status = other.status;
		this.reason = other.reason;
		this.statusInfo = other.statusInfo;
		this.failureCause=other.failureCause;
		set(other.get());
	}

	public Result<Value> apply( final Function<Value,Value> onSuccess ) {
		final Result<Value> result=new Result<Value>((Value)null);
		addCallback( new FutureCallback<Value>() {
			@Override
			public void onSuccess(Value v) {
				try {
					result.set(onSuccess.apply(v));
				} catch (Throwable t) {
					result.failureCause=t;
				}
			}
			@Override
			public void onFailure(Throwable t) {
				result.failureCause=t;
			}
		} );

		return result;
	}

	public Result<Value> apply( final Function<Value,Value> onSuccess, Executor executor ) {
		final Result<Value> result=new Result<Value>((Value)null);
		addCallback( new FutureCallback<Value>() {
			@Override
			public void onSuccess(Value v) {
				try {
					result.set(onSuccess.apply(v));
				} catch (Throwable t) {
					result.failureCause=t;
				}
			}
			@Override
			public void onFailure(Throwable t) {
				result.failureCause=t;
			}
		}, executor );

		return result;
	}

	public Result<Value> apply( final Function<Value,Value> onSuccess,
								final Function<Throwable,Value> onFailure ) {
		final Result<Value> result=new Result<Value>();
			addCallback( new FutureCallback<Value>() {
				@Override
				public void onSuccess(Value v) {
					try {
						result.set(onSuccess.apply(v));
					} catch (Throwable t) {
						result.failureCause=t;
					}
				}
				@Override
				public void onFailure(Throwable t) {
					try {
						result.set(onFailure.apply(t));
					} catch (Throwable t1) {
						result.failureCause=t1;
					}
				}
		} );

		return result;
	}

	public Result<Value> apply(final Function<Value,Value> onSuccess,
			final Function<Throwable,Value> onFailure,
			Executor executor) {
		final Result<Value> result = new Result<Value>();
		addCallback(new FutureCallback<Value>() {
			@Override
			public void onSuccess(Value v) {
				try {
					result.set(onSuccess.apply(v));
				} catch (Throwable t) {
					result.failureCause=t;
				}
			}

			@Override
			public void onFailure(Throwable t) {
				try {
					result.set(onFailure.apply(t));
				} catch (Throwable t1) {
					result.failureCause=t1;
				}
			}
		}, executor);

		return result;
	}
*/

func (result *result) Get() interface{} {
	/*
		try {
			if (result==null) {
				status=ResultStatus.FAILED;
			} else {
				Value got=((ListenableFuture<Value>)result).get();
				if (status==ResultStatus.PENDING)
					status=got==null?ResultStatus.FAILED:ResultStatus.SUCCESS;
				return got;
			}
		} catch (Throwable t) {
			status=ResultStatus.FAILED;
			reason=ResultStatusReason.NONE;
			statusInfo=t.getMessage();
			failureCause=t;
		}
	*/
	return nil
}

/*
	public String getReasonMessage() {
		getStatus();
		return ((getReason()==ResultStatusReason.NONE||getReason()==ResultStatusReason.UNEXPECTED)?"":getReason()) + (hasStatusInfo()?": " + getStatusInfo():"");
	}

	public String getReasonMessage( boolean includeStackTrace ) {
		getStatus();

		String stackTrace="";

		if (failureCause!=null&&includeStackTrace==true) {
			StringWriter stackWriter = new StringWriter();
			failureCause.printStackTrace(new PrintWriter(stackWriter));
			stackTrace=stackWriter.toString();
		}

		return ((getReason()==ResultStatusReason.NONE||getReason()==ResultStatusReason.UNEXPECTED)?"":getReason()) + (hasStatusInfo()?": " + getStatusInfo():"" + stackTrace );
	}

	public Throwable getFailureCause() {
		getStatus();
		return failureCause;
	}

	@Override
	public String toString() {
		return "Result [result=" + result + ", status=" + status + ", reason="
				+ reason + ", statusInfo=" + statusInfo + "]";
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return ((ListenableFuture<Value>)result).cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return ((ListenableFuture<Value>)result).isCancelled();
	}

	@Override
	public boolean isDone() {
		return ((ListenableFuture<Value>)result).isDone();
	}

	@Override
	public Value get(long timeout, TimeUnit unit) throws TimeoutException {
		try {
			Value got=((ListenableFuture<Value>)result).get(timeout, unit);
			status=ResultStatus.SUCCESS;
			return got;
		} catch (TimeoutException t) {
			throw t;
		} catch (Throwable t) {
			status=ResultStatus.FAILED;
			reason=ResultStatusReason.NONE;
			statusInfo=t.getMessage();
			failureCause=t;
		}

		return null;
	}

	public ResultStatusReason getReason() {

		if (getStatus()==ResultStatus.FAILED&&reason==ResultStatusReason.NONE) {
			if (failureCause!=null) {
				Throwable cause=(failureCause instanceof ExecutionException)?((ExecutionException)failureCause).getCause():failureCause;
				if (cause instanceof RequestProcessorException)
					reason=((RequestProcessorException)cause).getReason();
				if (cause instanceof EntityNotFoundException)
					reason=ResultStatusReason.ENTITY_NOT_FOUND;
				else if (cause instanceof NonUniqueResultException)
					reason=ResultStatusReason.DUPLICATE_ENTITIES_FOUND;
				else
					reason=ResultStatusReason.EXECUTION_EXCEPTION;
			}
		}

		return reason;
	}

	@Override
	public void addListener(Runnable listener, Executor executor) {
		if (result!=null)
			result.addListener(listener, executor);
	}

	public Result<Value> addCallback( FutureCallback<Value> callback ) {
		if (result==null)
			callback.onFailure(this.getFailureCause()!=null?this.getFailureCause():new InvalidResultException(this) );
		else
			Futures.addCallback(result,callback);
		return this;
	}

	public Result<Value> addCallback( FutureCallback<Value> callback, Executor executor ) {
		if (result==null)
			callback.onFailure(this.getFailureCause()!=null?this.getFailureCause():new InvalidResultException(this) );
		else
			Futures.addCallback(result,callback,executor);
		return this;
	}

	public void reset() {
		// TODO - we need to transfer the callbacks
		//  ?? not sure how
		result=SettableFuture.<Value>create();
	}

*/
