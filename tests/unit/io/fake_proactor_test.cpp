#include "support/fake_proactor.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace {

TEST(FakeProactor, SubmitRead_RegistraOperacionPendiente) {
    nexus::FakeProactor proactor;
    std::array<std::byte, 8> buffer{};
    proactor.submit_read(7, buffer, 42, [](nexus::Proactor::IoResult) {});

    ASSERT_EQ(proactor.pending(), 1U);
    const auto& op = proactor.peek(0);
    EXPECT_EQ(op.kind, nexus::FakeProactor::OpKind::Read);
    EXPECT_EQ(op.fd, nexus::NativeHandle{7});
    EXPECT_EQ(op.offset, 42U);
    EXPECT_EQ(op.read_buffer.size(), buffer.size());
}

TEST(FakeProactor, CompleteFront_InvocaLaCompletionConElResultado) {
    nexus::FakeProactor proactor;
    nexus::Proactor::IoResult got = 0;
    proactor.submit_fsync(3, false, [&](nexus::Proactor::IoResult result) { got = result; });

    proactor.complete_front(0);
    EXPECT_EQ(got, 0);
    EXPECT_EQ(proactor.pending(), 0U);
}

TEST(FakeProactor, RunCompletions_DrenaArmadasEnFIFOhastaMax) {
    nexus::FakeProactor proactor;
    std::vector<int> order;
    proactor.submit_send(1, {}, [&](nexus::Proactor::IoResult) { order.push_back(1); });
    proactor.submit_send(2, {}, [&](nexus::Proactor::IoResult) { order.push_back(2); });
    proactor.submit_send(3, {}, [&](nexus::Proactor::IoResult) { order.push_back(3); });

    proactor.arm_front(0);  // op 1
    proactor.arm_front(0);  // op 2
    proactor.arm_front(0);  // op 3

    EXPECT_EQ(proactor.run_completions(2), 2);  // tope: solo dos
    EXPECT_EQ(order, (std::vector<int>{1, 2}));
    EXPECT_EQ(proactor.run_completions(10), 1);  // la restante
    EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(proactor.run_completions(10), 0);  // ya no queda nada
}

TEST(FakeProactor, Wake_IncrementaContador) {
    nexus::FakeProactor proactor;
    EXPECT_EQ(proactor.wakes(), 0);
    proactor.wake();
    proactor.wake();
    EXPECT_EQ(proactor.wakes(), 2);
}

TEST(FakeProactor, WaitCompletions_NoBloquea_DrenaLoArmado) {
    nexus::FakeProactor proactor;
    int ran = 0;
    proactor.submit_send(1, {}, [&](nexus::Proactor::IoResult) { ++ran; });
    proactor.arm_front(0);

    // El doble no bloquea: ignora el deadline y drena como run_completions.
    EXPECT_EQ(proactor.wait_completions(8, nexus::MonoTime{}), 1);
    EXPECT_EQ(ran, 1);
    EXPECT_EQ(proactor.wait_completions(8, nexus::MonoTime{}), 0);  // nada más armado
}

}  // namespace
