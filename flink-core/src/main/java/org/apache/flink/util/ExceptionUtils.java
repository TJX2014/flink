/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// The function "stringifyException" is based on source code from the Hadoop Project (http://hadoop.apache.org/),
// licensed by the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership.
//

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.function.RunnableWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A collection of utility functions for dealing with exceptions and exception workflows.
 */
@Internal
public final class ExceptionUtils {

	/** The stringified representation of a null exception reference. */
	public static final String STRINGIFIED_NULL_EXCEPTION = "(null)";

	private static final String TM_DIRECT_OOM_ERROR_MESSAGE = String.format(
		"Direct buffer memory. The direct out-of-memory error has occurred. This can mean two things: either job(s) require(s) " +
			"a larger size of JVM direct memory or there is a direct memory leak. The direct memory can be " +
			"allocated by user code or some of its dependencies. In this case '%s' configuration option should be " +
			"increased. Flink framework and its dependencies also consume the direct memory, mostly for network " +
			"communication. The most of network memory is managed by Flink and should not result in out-of-memory " +
			"error. In certain special cases, in particular for jobs with high parallelism, the framework may " +
			"require more direct memory which is not managed by Flink. In this case '%s' configuration option " +
			"should be increased. If the error persists then there is probably a direct memory leak which has to " +
			"be investigated and fixed. The task executor has to be shutdown...",
		TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(),
		TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key());

	private static final String TM_METASPACE_OOM_ERROR_MESSAGE = String.format(
		"Metaspace. The metaspace out-of-memory error has occurred. This can mean two things: either the job requires " +
			"a larger size of JVM metaspace to load classes or there is a class loading leak. In the first case " +
			"'%s' configuration option should be increased. If the error persists (usually in cluster after " +
			"several job (re-)submissions) then there is probably a class loading leak which has to be " +
			"investigated and fixed. The task executor has to be shutdown...",
		TaskManagerOptions.JVM_METASPACE.key());

	/**
	 * Makes a string representation of the exception's stack trace, or "(null)", if the
	 * exception is null.
	 *
	 * <p>This method makes a best effort and never fails.
	 *
	 * @param e The exception to stringify.
	 * @return A string with exception name and call stack.
	 */
	public static String stringifyException(final Throwable e) {
		if (e == null) {
			return STRINGIFIED_NULL_EXCEPTION;
		}

		try {
			StringWriter stm = new StringWriter();
			PrintWriter wrt = new PrintWriter(stm);
			e.printStackTrace(wrt);
			wrt.close();
			return stm.toString();
		}
		catch (Throwable t) {
			return e.getClass().getName() + " (error while printing stack trace)";
		}
	}

	/**
	 * Checks whether the given exception indicates a situation that may leave the
	 * JVM in a corrupted state, meaning a state where continued normal operation can only be
	 * guaranteed via clean process restart.
	 *
	 * <p>Currently considered fatal exceptions are Virtual Machine errors indicating
	 * that the JVM is corrupted, like {@link InternalError}, {@link UnknownError},
	 * and {@link java.util.zip.ZipError} (a special case of InternalError).
	 * The {@link ThreadDeath} exception is also treated as a fatal error, because when
	 * a thread is forcefully stopped, there is a high chance that parts of the system
	 * are in an inconsistent state.
	 *
	 * @param t The exception to check.
	 * @return True, if the exception is considered fatal to the JVM, false otherwise.
	 */
	public static boolean isJvmFatalError(Throwable t) {
		return (t instanceof InternalError) || (t instanceof UnknownError) || (t instanceof ThreadDeath);
	}

	/**
	 * Checks whether the given exception indicates a situation that may leave the
	 * JVM in a corrupted state, or an out-of-memory error.
	 *
	 * <p>See {@link ExceptionUtils#isJvmFatalError(Throwable)} for a list of fatal JVM errors.
	 * This method additionally classifies the {@link OutOfMemoryError} as fatal, because it
	 * may occur in any thread (not the one that allocated the majority of the memory) and thus
	 * is often not recoverable by destroying the particular thread that threw the exception.
	 *
	 * @param t The exception to check.
	 * @return True, if the exception is fatal to the JVM or and OutOfMemoryError, false otherwise.
	 */
	public static boolean isJvmFatalOrOutOfMemoryError(Throwable t) {
		return isJvmFatalError(t) || t instanceof OutOfMemoryError;
	}

	/**
	 * Tries to enrich the passed exception with additional information.
	 *
	 * <p>This method improves error message for direct and metaspace {@link OutOfMemoryError}.
	 * It adds description of possible causes and ways of resolution.
	 *
	 * @param exception exception to enrich if not {@code null}
	 * @return the enriched exception or the original if no additional information could be added;
	 * {@code null} if the argument was {@code null}
	 */
	@Nullable
	public static Throwable tryEnrichTaskManagerError(@Nullable Throwable exception) {
		if (exception instanceof OutOfMemoryError) {
			return tryEnrichTaskManagerOutOfMemoryError((OutOfMemoryError) exception);
		}

		return exception;
	}

	private static Throwable tryEnrichTaskManagerOutOfMemoryError(OutOfMemoryError oom) {
		if (isMetaspaceOutOfMemoryError(oom)) {
			return changeOutOfMemoryErrorMessage(oom, TM_METASPACE_OOM_ERROR_MESSAGE);
		} else if (isDirectOutOfMemoryError(oom)) {
			return changeOutOfMemoryErrorMessage(oom, TM_DIRECT_OOM_ERROR_MESSAGE);
		}

		return oom;
	}

	private static OutOfMemoryError changeOutOfMemoryErrorMessage(OutOfMemoryError oom, String newMessage) {
		if (oom.getMessage().equals(newMessage)) {
			return oom;
		}
		OutOfMemoryError newError = new OutOfMemoryError(newMessage);
		newError.initCause(oom.getCause());
		newError.setStackTrace(oom.getStackTrace());
		return newError;
	}

	/**
	 * Checks whether the given exception indicates a JVM metaspace out-of-memory error.
	 *
	 * @param t The exception to check.
	 * @return True, if the exception is the metaspace {@link OutOfMemoryError}, false otherwise.
	 */
	public static boolean isMetaspaceOutOfMemoryError(@Nullable Throwable t) {
		return isOutOfMemoryErrorWithMessageStartingWith(t, "Metaspace");
	}

	/**
	 * Checks whether the given exception indicates a JVM direct out-of-memory error.
	 *
	 * @param t The exception to check.
	 * @return True, if the exception is the direct {@link OutOfMemoryError}, false otherwise.
	 */
	public static boolean isDirectOutOfMemoryError(@Nullable Throwable t) {
		return isOutOfMemoryErrorWithMessageStartingWith(t, "Direct buffer memory");
	}

	private static boolean isOutOfMemoryErrorWithMessageStartingWith(@Nullable Throwable t, String prefix) {
		// the exact matching of the class is checked to avoid matching any custom subclasses of OutOfMemoryError
		// as we are interested in the original exceptions, generated by JVM.
		return isOutOfMemoryError(t) && t.getMessage() != null && t.getMessage().startsWith(prefix);
	}

	private static boolean isOutOfMemoryError(@Nullable Throwable t) {
		return t != null && t.getClass() == OutOfMemoryError.class;
	}

	/**
	 * Rethrows the given {@code Throwable}, if it represents an error that is fatal to the JVM.
	 * See {@link ExceptionUtils#isJvmFatalError(Throwable)} for a definition of fatal errors.
	 *
	 * @param t The Throwable to check and rethrow.
	 */
	public static void rethrowIfFatalError(Throwable t) {
		if (isJvmFatalError(t)) {
			throw (Error) t;
		}
	}

	/**
	 * Rethrows the given {@code Throwable}, if it represents an error that is fatal to the JVM
	 * or an out-of-memory error. See {@link ExceptionUtils#isJvmFatalError(Throwable)} for a
	 * definition of fatal errors.
	 *
	 * @param t The Throwable to check and rethrow.
	 */
	public static void rethrowIfFatalErrorOrOOM(Throwable t) {
		if (isJvmFatalError(t) || t instanceof OutOfMemoryError) {
			throw (Error) t;
		}
	}

	/**
	 * Adds a new exception as a {@link Throwable#addSuppressed(Throwable) suppressed exception}
	 * to a prior exception, or returns the new exception, if no prior exception exists.
	 *
	 * <pre>{@code
	 *
	 * public void closeAllThings() throws Exception {
	 *     Exception ex = null;
	 *     try {
	 *         component.shutdown();
	 *     } catch (Exception e) {
	 *         ex = firstOrSuppressed(e, ex);
	 *     }
	 *     try {
	 *         anotherComponent.stop();
	 *     } catch (Exception e) {
	 *         ex = firstOrSuppressed(e, ex);
	 *     }
	 *     try {
	 *         lastComponent.shutdown();
	 *     } catch (Exception e) {
	 *         ex = firstOrSuppressed(e, ex);
	 *     }
	 *
	 *     if (ex != null) {
	 *         throw ex;
	 *     }
	 * }
	 * }</pre>
	 *
	 * @param newException The newly occurred exception
	 * @param previous     The previously occurred exception, possibly null.
	 *
	 * @return The new exception, if no previous exception exists, or the previous exception with the
	 *         new exception in the list of suppressed exceptions.
	 */
	public static <T extends Throwable> T firstOrSuppressed(T newException, @Nullable T previous) {
		checkNotNull(newException, "newException");

		if (previous == null) {
			return newException;
		} else {
			previous.addSuppressed(newException);
			return previous;
		}
	}

	/**
	 * Throws the given {@code Throwable} in scenarios where the signatures do not allow you to
	 * throw an arbitrary Throwable. Errors and RuntimeExceptions are thrown directly, other exceptions
	 * are packed into runtime exceptions
	 *
	 * @param t The throwable to be thrown.
	 */
	public static void rethrow(Throwable t) {
		if (t instanceof Error) {
			throw (Error) t;
		}
		else if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		}
		else {
			throw new RuntimeException(t);
		}
	}

	/**
	 * Throws the given {@code Throwable} in scenarios where the signatures do not allow you to
	 * throw an arbitrary Throwable. Errors and RuntimeExceptions are thrown directly, other exceptions
	 * are packed into a parent RuntimeException.
	 *
	 * @param t The throwable to be thrown.
	 * @param parentMessage The message for the parent RuntimeException, if one is needed.
	 */
	public static void rethrow(Throwable t, String parentMessage) {
		if (t instanceof Error) {
			throw (Error) t;
		}
		else if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		}
		else {
			throw new RuntimeException(parentMessage, t);
		}
	}

	/**
	 * Throws the given {@code Throwable} in scenarios where the signatures do allow to
	 * throw a Exception. Errors and Exceptions are thrown directly, other "exotic"
	 * subclasses of Throwable are wrapped in an Exception.
	 *
	 * @param t The throwable to be thrown.
	 * @param parentMessage The message for the parent Exception, if one is needed.
	 */
	public static void rethrowException(Throwable t, String parentMessage) throws Exception {
		if (t instanceof Error) {
			throw (Error) t;
		}
		else if (t instanceof Exception) {
			throw (Exception) t;
		}
		else {
			throw new Exception(parentMessage, t);
		}
	}

	/**
	 * Throws the given {@code Throwable} in scenarios where the signatures do allow to
	 * throw a Exception. Errors and Exceptions are thrown directly, other "exotic"
	 * subclasses of Throwable are wrapped in an Exception.
	 *
	 * @param t The throwable to be thrown.
	 */
	public static void rethrowException(Throwable t) throws Exception {
		if (t instanceof Error) {
			throw (Error) t;
		}
		else if (t instanceof Exception) {
			throw (Exception) t;
		}
		else {
			throw new Exception(t.getMessage(), t);
		}
	}

	/**
	 * Tries to throw the given exception if not null.
	 *
	 * @param e exception to throw if not null.
	 * @throws Exception
	 */
	public static void tryRethrowException(@Nullable Exception e) throws Exception {
		if (e != null) {
			throw e;
		}
	}

	/**
	 * Tries to throw the given {@code Throwable} in scenarios where the signatures allows only IOExceptions
	 * (and RuntimeException and Error). Throws this exception directly, if it is an IOException,
	 * a RuntimeException, or an Error. Otherwise does nothing.
	 *
	 * @param t The Throwable to be thrown.
	 */
	public static void tryRethrowIOException(Throwable t) throws IOException {
		if (t instanceof IOException) {
			throw (IOException) t;
		}
		else if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		}
		else if (t instanceof Error) {
			throw (Error) t;
		}
	}

	/**
	 * Re-throws the given {@code Throwable} in scenarios where the signatures allows only IOExceptions
	 * (and RuntimeException and Error).
	 *
	 * <p>Throws this exception directly, if it is an IOException, a RuntimeException, or an Error. Otherwise it
	 * wraps it in an IOException and throws it.
	 *
	 * @param t The Throwable to be thrown.
	 */
	public static void rethrowIOException(Throwable t) throws IOException {
		if (t instanceof IOException) {
			throw (IOException) t;
		}
		else if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		}
		else if (t instanceof Error) {
			throw (Error) t;
		}
		else {
			throw new IOException(t.getMessage(), t);
		}
	}

	/**
	 * Checks whether a throwable chain contains a specific type of exception and returns it. It deserializes
	 * any {@link SerializedThrowable} that are found using the provided {@link ClassLoader}.
	 *
	 * @param throwable the throwable chain to check.
	 * @param searchType the type of exception to search for in the chain.
	 * @param classLoader to use for deserialization.
	 * @return Optional throwable of the requested type if available, otherwise empty
	 */
	public static <T extends Throwable> Optional<T> findSerializedThrowable(Throwable throwable, Class<T> searchType, ClassLoader classLoader) {
		if (throwable == null || searchType == null) {
			return Optional.empty();
		}

		Throwable t = throwable;
		while (t != null) {
			if (searchType.isAssignableFrom(t.getClass())) {
				return Optional.of(searchType.cast(t));
			} else if (t.getClass().isAssignableFrom(SerializedThrowable.class)) {
				Throwable next = ((SerializedThrowable) t).deserializeError(classLoader);
				// SerializedThrowable#deserializeError returns itself under some conditions (e.g., null cause).
				// If that happens, exit to avoid looping infinitely. This is ok because if the user was searching
				// for a SerializedThrowable, we would have returned it in the initial if condition.
				t = (next == t) ? null : next;
			} else {
				t = t.getCause();
			}
		}

		return Optional.empty();
	}

	/**
	 * Checks whether a throwable chain contains a specific type of exception and returns it.
	 *
	 * @param throwable the throwable chain to check.
	 * @param searchType the type of exception to search for in the chain.
	 * @return Optional throwable of the requested type if available, otherwise empty
	 */
	public static <T extends Throwable> Optional<T> findThrowable(Throwable throwable, Class<T> searchType) {
		if (throwable == null || searchType == null) {
			return Optional.empty();
		}

		Throwable t = throwable;
		while (t != null) {
			if (searchType.isAssignableFrom(t.getClass())) {
				return Optional.of(searchType.cast(t));
			} else {
				t = t.getCause();
			}
		}

		return Optional.empty();
	}

	/**
	 * Checks whether a throwable chain contains a specific type of exception and returns it.
	 * This method handles {@link SerializedThrowable}s in the chain and deserializes them with
	 * the given ClassLoader.
	 *
	 * <p>SerializedThrowables are often used when exceptions might come from dynamically loaded code and
	 * be transported over RPC / HTTP for better error reporting.
	 * The receiving processes or threads might not have the dynamically loaded code available.
	 *
	 * @param throwable the throwable chain to check.
	 * @param searchType the type of exception to search for in the chain.
	 * @param classLoader the ClassLoader to use when encountering a SerializedThrowable.
	 * @return Optional throwable of the requested type if available, otherwise empty
	 */
	public static <T extends Throwable> Optional<T> findThrowableSerializedAware(
			Throwable throwable,
			Class<T> searchType,
			ClassLoader classLoader) {

		if (throwable == null || searchType == null) {
			return Optional.empty();
		}

		Throwable t = throwable;
		while (t != null) {
			if (searchType.isAssignableFrom(t.getClass())) {
				return Optional.of(searchType.cast(t));
			} else if (t instanceof SerializedThrowable) {
				t = ((SerializedThrowable) t).deserializeError(classLoader);
			} else {
				t = t.getCause();
			}
		}

		return Optional.empty();
	}

	/**
	 * Checks whether a throwable chain contains an exception matching a predicate and returns it.
	 *
	 * @param throwable the throwable chain to check.
	 * @param predicate the predicate of the exception to search for in the chain.
	 * @return Optional throwable of the requested type if available, otherwise empty
	 */
	public static Optional<Throwable> findThrowable(Throwable throwable, Predicate<Throwable> predicate) {
		if (throwable == null || predicate == null) {
			return Optional.empty();
		}

		Throwable t = throwable;
		while (t != null) {
			if (predicate.test(t)) {
				return Optional.of(t);
			} else {
				t = t.getCause();
			}
		}

		return Optional.empty();
	}

	/**
	 * Checks whether a throwable chain contains a specific error message and returns the corresponding throwable.
	 *
	 * @param throwable the throwable chain to check.
	 * @param searchMessage the error message to search for in the chain.
	 * @return Optional throwable containing the search message if available, otherwise empty
	 */
	public static Optional<Throwable> findThrowableWithMessage(Throwable throwable, String searchMessage) {
		if (throwable == null || searchMessage == null) {
			return Optional.empty();
		}

		Throwable t = throwable;
		while (t != null) {
			if (t.getMessage() != null && t.getMessage().contains(searchMessage)) {
				return Optional.of(t);
			} else {
				t = t.getCause();
			}
		}

		return Optional.empty();
	}

	/**
	 * Unpacks an {@link ExecutionException} and returns its cause. Otherwise the given
	 * Throwable is returned.
	 *
	 * @param throwable to unpack if it is an ExecutionException
	 * @return Cause of ExecutionException or given Throwable
	 */
	public static Throwable stripExecutionException(Throwable throwable) {
		return stripException(throwable, ExecutionException.class);
	}

	/**
	 * Unpacks an {@link CompletionException} and returns its cause. Otherwise the given
	 * Throwable is returned.
	 *
	 * @param throwable to unpack if it is an CompletionException
	 * @return Cause of CompletionException or given Throwable
	 */
	public static Throwable stripCompletionException(Throwable throwable) {
		return stripException(throwable, CompletionException.class);
	}

	/**
	 * Unpacks an specified exception and returns its cause. Otherwise the given
	 * {@link Throwable} is returned.
	 *
	 * @param throwableToStrip to strip
	 * @param typeToStrip type to strip
	 * @return Unpacked cause or given Throwable if not packed
	 */
	public static Throwable stripException(Throwable throwableToStrip, Class<? extends Throwable> typeToStrip) {
		while (typeToStrip.isAssignableFrom(throwableToStrip.getClass()) && throwableToStrip.getCause() != null) {
			throwableToStrip = throwableToStrip.getCause();
		}

		return throwableToStrip;
	}

	/**
	 * Tries to find a {@link SerializedThrowable} as the cause of the given throwable and throws its
	 * deserialized value. If there is no such throwable, then the original throwable is thrown.
	 *
	 * @param throwable to check for a SerializedThrowable
	 * @param classLoader to be used for the deserialization of the SerializedThrowable
	 * @throws Throwable either the deserialized throwable or the given throwable
	 */
	public static void tryDeserializeAndThrow(Throwable throwable, ClassLoader classLoader) throws Throwable {
		Throwable current = throwable;

		while (!(current instanceof SerializedThrowable) && current.getCause() != null) {
			current = current.getCause();
		}

		if (current instanceof SerializedThrowable) {
			throw ((SerializedThrowable) current).deserializeError(classLoader);
		} else {
			throw throwable;
		}
	}

	/**
	 * Checks whether the given exception is a {@link InterruptedException} and sets
	 * the interrupted flag accordingly.
	 *
	 * @param e to check whether it is an {@link InterruptedException}
	 */
	public static void checkInterrupted(Throwable e) {
		if (e instanceof InterruptedException) {
			Thread.currentThread().interrupt();
		}
	}

	// ------------------------------------------------------------------------
	//  Lambda exception utilities
	// ------------------------------------------------------------------------

	public static void suppressExceptions(RunnableWithException action) {
		try {
			action.run();
		}
		catch (InterruptedException e) {
			// restore interrupted state
			Thread.currentThread().interrupt();
		}
		catch (Throwable t) {
			if (isJvmFatalError(t)) {
				rethrow(t);
			}
		}
	}

	// ------------------------------------------------------------------------

	/** Private constructor to prevent instantiation. */
	private ExceptionUtils() {}
}
