/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.*;
import static reactor.core.scheduler.Schedulers.parallel;

public class FluxPeekFuseableTest {

	@Test
	public void nullSource() {
		assertThatNullPointerException()
				.isThrownBy(() -> new FluxPeekFuseable<>(null, null, null,
						null, null, null, null, null));
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.just(1),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).as("onSubscribe").isNotNull();
		assertThat(onNext.get()).as("onNext").isEqualTo(1);
		assertThat(onError.get()).as("onError").isNull();
		assertThat(onComplete.get()).as("onComplete").isTrue();
		assertThat(onAfterComplete.get()).as("onAfterComplete").isTrue();
		assertThat(onRequest.get()).as("onRequest").isEqualTo(Long.MAX_VALUE);
		assertThat(onCancel.get()).as("onCancel").isFalse();
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.error(new RuntimeException("forced failure")),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).as("onSubscribe").isNotNull();
		assertThat(onNext.get()).as("onNext").isNull();
		assertThat(onError.get()).as("onError").isInstanceOf(RuntimeException.class);
		assertThat(onComplete.get()).as("onComplete").isFalse();
		assertThat(onAfterComplete.get()).as("onAfterComplete").isTrue();
		assertThat(onRequest.get()).as("onRequest").isEqualTo(Long.MAX_VALUE);
		assertThat(onCancel.get()).as("onCancel").isFalse();
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.empty(),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).as("onSubscribe").isNotNull();
		assertThat(onNext.get()).as("onNext").isNull();
		assertThat(onError.get()).as("onError").isNull();
		assertThat(onComplete.get()).as("onComplete").isTrue();
		assertThat(onAfterComplete.get()).as("onAfterComplete").isTrue();
		assertThat(onRequest.get()).as("onRequest").isEqualTo(Long.MAX_VALUE);
		assertThat(onCancel.get()).as("onCancel").isFalse();
	}

	@Test
	public void never() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.never(),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).as("onSubscribe").isNotNull();
		assertThat(onNext.get()).as("onNext").isNull();
		assertThat(onError.get()).as("onError").isNull();
		assertThat(onComplete.get()).as("onComplete").isFalse();
		assertThat(onAfterComplete.get()).as("onAfterComplete").isFalse();
		assertThat(onRequest.get()).as("onRequest").isEqualTo(Long.MAX_VALUE);
		assertThat(onCancel.get()).as("onCancel").isFalse();
	}

	@Test
	public void neverCancel() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.never(),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).as("onSubscribe").isNotNull();
		assertThat(onNext.get()).as("onNext").isNull();
		assertThat(onError.get()).as("onError").isNull();
		assertThat(onComplete.get()).as("onComplete").isFalse();
		assertThat(onAfterComplete.get()).as("onAfterComplete").isFalse();
		assertThat(onRequest.get()).as("onRequest").isEqualTo(Long.MAX_VALUE);
		assertThat(onCancel.get()).as("onCancel").isFalse();

		ts.cancel();

		assertThat(onCancel.get()).as("onCancel post cancel").isTrue();
	}

	@Test
	public void callbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Throwable err = new Exception("test");

		Flux.just(1)
		    .doOnNext(d -> {
			    throw Exceptions.propagate(err);
		    })
		    .subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = AssertSubscriber.create();

		try {
			Flux.just(1)
			    .doOnNext(d -> {
				    throw Exceptions.bubble(err);
			    })
			    .subscribe(ts);

			fail("");
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).isSameAs(err);
		}
	}

	@Test
	public void completeCallbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Throwable err = new Exception("test");

		Flux.just(1)
		    .doOnComplete(() -> {
			    throw Exceptions.propagate(err);
		    })
		    .subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = AssertSubscriber.create();

		try {
			Flux.just(1)
			    .doOnComplete(() -> {
				    throw Exceptions.bubble(err);
			    })
			    .subscribe(ts);

			fail("");
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).isSameAs(err);
		}
	}

	@Test
	public void errorCallbackError() {
		IllegalStateException err = new IllegalStateException("test");

		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.error(new IllegalArgumentException("bar")), null, null,
				e -> { throw err; },
				null, null, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();
		flux.subscribe(ts);

		ts.assertNoValues();
		ts.assertError(IllegalStateException.class);
		ts.assertErrorWith(e -> e.getSuppressed()[0].getMessage().equals("bar"));
	}

	//See https://github.com/reactor/reactor-core/issues/272
	@Test
	public void errorCallbackError2() {
		//test with alternate / wrapped error types

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Throwable err = new Exception("test");

		Flux.just(1)
		    .doOnNext(d -> {
			    throw new RuntimeException();
		    })
		    .doOnError(e -> {
			    throw Exceptions.propagate(err);
		    })
		    .subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = AssertSubscriber.create();
		try {
			Flux.just(1)
			    .doOnNext(d -> {
				    throw new RuntimeException();
			    })
			    .doOnError(d -> {
				    throw Exceptions.bubble(err);
			    })
			    .subscribe(ts);

			fail("");
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).isSameAs(err);
		}
	}

	//See https://github.com/reactor/reactor-core/issues/253
	@Test
	public void errorCallbackErrorWithParallel() {
		AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();

		Mono.just(1)
		    .publishOn(parallel())
		    .doOnNext(i -> {
			    throw new IllegalArgumentException();
		    })
		    .doOnError(e -> {
			    throw new IllegalStateException(e);
		    })
		    .subscribe(assertSubscriber);

		assertSubscriber
				.await()
				.assertError(IllegalStateException.class)
				.assertNotComplete();
	}

	@Test
	public void afterTerminateCallbackErrorDoesNotInvokeOnError() {
		IllegalStateException err = new IllegalStateException("test");
		AtomicReference<Throwable> errorCallbackCapture = new AtomicReference<>();

		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.empty(), null, null, errorCallbackCapture::set, null,
				() -> { throw err; }, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Exception e) {
			assertThat(e).hasCause(err);
		}
		ts.assertNoValues();
		ts.assertComplete();

		//the onError wasn't invoked:
		assertThat(errorCallbackCapture.get()).isNull();
	}

	@Test
	public void afterTerminateCallbackFatalIsThrownDirectly() {
		AtomicReference<Throwable> errorCallbackCapture = new AtomicReference<>();
		Error fatal = new LinkageError();
		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.empty(), null, null, errorCallbackCapture::set, null,
				() -> { throw fatal; }, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Throwable e) {
			assertThat(e).isSameAs(fatal);
		}
		ts.assertNoValues();
		ts.assertComplete();

		assertThat(errorCallbackCapture.get()).isNull();


		//same with after error
		errorCallbackCapture.set(null);
		flux = new FluxPeekFuseable<>(
				Flux.error(new NullPointerException()), null, null, errorCallbackCapture::set, null,
				() -> { throw fatal; }, null, null);

		ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Throwable e) {
			assertThat(e).isSameAs(fatal);
		}
		ts.assertNoValues();
		ts.assertError(NullPointerException.class);

		assertThat(errorCallbackCapture.get()).isInstanceOf(NullPointerException.class);
	}

	@Test
	public void afterTerminateCallbackErrorAndErrorCallbackError() {
		IllegalStateException err = new IllegalStateException("expected afterTerminate");
		IllegalArgumentException err2 = new IllegalArgumentException("error");

		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.empty(), null, null, e -> { throw err2; },
				null,
				() -> { throw err; }, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Exception e) {
			e.printStackTrace();
			assertThat(e).hasCause(err);
			assertThat(err2).hasNoSuppressedExceptions();
			//err2 is never thrown
		}
		ts.assertNoValues();
		ts.assertComplete();
	}

	@Test
	public void afterTerminateCallbackErrorAndErrorCallbackError2() {
		IllegalStateException afterTerminate = new IllegalStateException("afterTerminate");
		IllegalArgumentException error = new IllegalArgumentException("error");
		NullPointerException err = new NullPointerException();

		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.error(err),
				null, null,
				e -> { throw error; }, null, () -> { throw afterTerminate; },
				null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Exception e) {
			assertThat(e).hasCause(afterTerminate);
			//afterTerminate suppressed error which itself suppressed original err
			assertThat(afterTerminate).hasSuppressedException(error);
			assertThat(error).hasSuppressedException(err);
		}
		ts.assertNoValues();
		//the subscriber still sees the 'error' message since actual.onError is called before the afterTerminate callback
		ts.assertErrorMessage("error");
	}


	@Test
	public void syncFusionAvailable() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .doOnNext(v -> {
		    })
		    .subscribe(ts);

		Subscription s = ts.upstream();
		assertThat(s).withFailMessage("Non-fuseable upstream: " + s)
		             .isInstanceOf(Fuseable.QueueSubscription.class);
	}

	@Test
	public void asyncFusionAvailable() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		UnicastProcessor.create(Queues.<Integer>get(2).get())
		                .doOnNext(v -> {
		                })
		                .subscribe(ts);

		Subscription s = ts.upstream();
		assertThat(s).withFailMessage("Non-fuseable upstream: " + s)
		             .isInstanceOf(Fuseable.QueueSubscription.class);
	}

	@Test
	public void conditionalFusionAvailable() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.from(u -> {
			if (!(u instanceof Fuseable.ConditionalSubscriber)) {
				Operators.error(u,
						new IllegalArgumentException("The subscriber is not conditional: " + u));
			}
			else {
				Operators.complete(u);
			}
		})
		          .doOnNext(v -> {
		          })
		          .filter(v -> true)
		          .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertComplete();
	}

	@Test
	public void conditionalFusionAvailableWithFuseable() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.wrap(u -> {
			if (!(u instanceof Fuseable.ConditionalSubscriber)) {
				Operators.error(u,
						new IllegalArgumentException("The subscriber is not conditional: " + u));
			}
			else {
				Operators.complete(u);
			}
		})
		    .doOnNext(v -> {
		          })
		    .filter(v -> true)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertComplete();
	}

	//TODO was these 2 tests supposed to trigger sync fusion and go through poll() ?
	@Test
	public void noFusionCompleteCalled() {
		AtomicBoolean onComplete = new AtomicBoolean();

		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .doOnComplete(() -> onComplete.set(true))
		    .subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2)
		  .assertComplete();

		assertThat(onComplete.get()).withFailMessage("onComplete not called back").isTrue();
	}

	@Test
	public void noFusionAfterTerminateCalled() {
		AtomicBoolean onTerminate = new AtomicBoolean();

		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .doAfterTerminate(() -> onTerminate.set(true))
		    .subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2)
		  .assertComplete();

		assertThat(onTerminate.get()).withFailMessage("onAfterTerminate not called back").isTrue();
	}

	@Test
	public void syncPollCompleteCalled() {
		AtomicBoolean onComplete = new AtomicBoolean();
		ConnectableFlux<Integer> f = Flux.just(1)
		                                .doOnComplete(() -> onComplete.set(true))
		                                .publish();
		StepVerifier.create(f)
		            .then(f::connect)
		            .expectNext(1)
		            .verifyComplete();

		assertThat(onComplete.get()).withFailMessage("onComplete not called back").isTrue();
	}

	@Test
	public void syncPollConditionalCompleteCalled() {
		AtomicBoolean onComplete = new AtomicBoolean();
		ConnectableFlux<Integer> f = Flux.just(1)
		                                .doOnComplete(() -> onComplete.set(true))
		                                .filter(v -> true)
		                                .publish();
		StepVerifier.create(f)
		            .then(f::connect)
		            .expectNext(1)
		            .verifyComplete();

		assertThat(onComplete.get()).withFailMessage("onComplete not called back").isTrue();
	}

	@Test
	public void syncPollAfterTerminateCalled() {
		AtomicBoolean onAfterTerminate = new AtomicBoolean();
		ConnectableFlux<Integer> f = Flux.just(1)
		                                .doAfterTerminate(() -> onAfterTerminate.set(true))
		                                .publish();
		StepVerifier.create(f)
		            .then(f::connect)
		            .expectNext(1)
		            .verifyComplete();

		assertThat(onAfterTerminate.get()).withFailMessage("onAfterTerminate not called back").isTrue();
	}

	@Test
	public void syncPollConditionalAfterTerminateCalled() {
		AtomicBoolean onAfterTerminate = new AtomicBoolean();
		ConnectableFlux<Integer> f = Flux.just(1)
		                                .doAfterTerminate(() -> onAfterTerminate.set(true))
		                                .filter(v -> true)
		                                .publish();
		StepVerifier.create(f)
		            .then(f::connect)
		            .expectNext(1)
		            .verifyComplete();

		assertThat(onAfterTerminate.get()).withFailMessage("onAfterTerminate not called back").isTrue();
	}

	@Test
	public void fusedDoOnNextOnErrorBothFailing() {
		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError", e);
		                                 })
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorMatches(e -> e instanceof IllegalStateException
				            && "fromOnError".equals(e.getMessage())
				            && e.getCause() instanceof IllegalArgumentException
				            && "fromOnNext".equals(e.getCause().getMessage()));
	}

	@Test
	public void fusedDoOnNextOnErrorDoOnErrorAllFailing() {
		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError", e);
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError2", e);
		                                 })
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorSatisfies(e -> {
					            assertThat(e)
					                      .isInstanceOf(IllegalStateException.class)
					                      .hasMessage("fromOnError2")
					                      .hasCauseInstanceOf(IllegalStateException.class);
					            assertThat(e.getCause())
					                      .hasMessage("fromOnError")
					                      .hasCauseInstanceOf(IllegalArgumentException.class);
					            assertThat(e.getCause().getCause())
					                      .hasMessage("fromOnNext");
				            });
	}

	@Test
	public void fusedDoOnNextCallsOnErrorWhenFailing() {
		AtomicBoolean passedOnError = new AtomicBoolean();

		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> passedOnError.set(true))
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorMatches(e -> e instanceof IllegalArgumentException
				            && "fromOnNext".equals(e.getMessage()));

		assertThat(passedOnError.get()).isTrue();
	}

	@Test
	public void conditionalFusedDoOnNextOnErrorBothFailing() {
		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError", e);
		                                 })
		                                 .filter(v -> true)
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorMatches(e -> e instanceof IllegalStateException
				            && "fromOnError".equals(e.getMessage())
				            && e.getCause() instanceof IllegalArgumentException
				            && "fromOnNext".equals(e.getCause().getMessage()));
	}

	@Test
	public void conditionalFusedDoOnNextOnErrorDoOnErrorAllFailing() {
		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError", e);
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError2", e);
		                                 })
		                                 .filter(v -> true)
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorSatisfies(e -> {
					            assertThat(e)
					                      .isInstanceOf(IllegalStateException.class)
					                      .hasMessage("fromOnError2")
					                      .hasCauseInstanceOf(IllegalStateException.class);
					            assertThat(e.getCause())
					                      .hasMessage("fromOnError")
					                      .hasCauseInstanceOf(IllegalArgumentException.class);
					            assertThat(e.getCause().getCause())
					                      .hasMessage("fromOnNext");
				            });
	}

	@Test
	public void conditionalFusedDoOnNextCallsOnErrorWhenFailing() {
		AtomicBoolean passedOnError = new AtomicBoolean();

		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> passedOnError.set(true))
		                                 .filter(v -> true)
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorMatches(e -> e instanceof IllegalArgumentException
				            && "fromOnNext".equals(e.getMessage()));

		assertThat(passedOnError.get()).isTrue();
	}

	@Test
	public void should_reduce_to_10_events() {
		for (int i = 0; i < 20; i++) {
			int n = i;
			List<Integer> rs = Collections.synchronizedList(new ArrayList<>());
			AtomicInteger count = new AtomicInteger();
			Flux.range(0, 10)
			    .flatMap(x -> Flux.range(0, 2)
			                      .doOnNext(rs::add)
			                      .map(y -> blockingOp(x, y))
			                      .subscribeOn(Schedulers.parallel())
			                      .reduce((l, r) -> l + "_" + r +" ("+x+", it:"+n+")")
			    )
			    .doOnNext(s -> {
				    count.incrementAndGet();
			    })
			    .blockLast();

			assertThat(count).hasValue(10);
		}
	}

	@Test
	public void should_reduce_to_10_events_conditional() {
		for (int i = 0; i < 20; i++) {
			AtomicInteger count = new AtomicInteger();
			Flux.range(0, 10)
			    .flatMap(x -> Flux.range(0, 2)
			                      .map(y -> blockingOp(x, y))
			                      .subscribeOn(Schedulers.parallel())
			                      .reduce((l, r) -> l + "_" + r)
			                      .doOnSuccess(s -> {
				                      count.incrementAndGet();
			                      })
			                      .filter(v -> true))
			    .blockLast();

			assertThat(count).hasValue(10);
		}
	}

	static String blockingOp(Integer x, Integer y) {
		try {
			sleep(10);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		return "x" + x + "y" + y;
	}


	@Test
    public void scanFuseableSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxPeek<Integer> peek = new FluxPeek<>(Flux.just(1), s -> {}, s -> {},
        		e -> {}, () -> {}, () -> {}, r -> {}, () -> {});
        FluxPeekFuseable.PeekFuseableSubscriber<Integer> test = new FluxPeekFuseable.PeekFuseableSubscriber<>(actual, peek);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void scanFuseableConditionalSubscriber() {
	    @SuppressWarnings("unchecked")
	    Fuseable.ConditionalSubscriber<Integer> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FluxPeek<Integer> peek = new FluxPeek<>(Flux.just(1), s -> {}, s -> {},
        		e -> {}, () -> {}, () -> {}, r -> {}, () -> {});
        FluxPeekFuseable.PeekFuseableConditionalSubscriber<Integer> test =
        		new FluxPeekFuseable.PeekFuseableConditionalSubscriber<>(actual, peek);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }
}
