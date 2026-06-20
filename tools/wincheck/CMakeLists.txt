# tools/wincheck — arnés de verificación en RUNTIME de nexus-io en Windows (F10, ADR-0022).
# Windows-only: ejercita File (E/S bloqueante), IocpBackend + Socket/Listener (eco por loopback,
# bombeando las completions a mano) y submit_timer, SIN ReactorPool (POSIX) ni ctest. En Linux este
# CMakeLists no define ningún target (la guarda WIN32 lo deja vacío), así que no afecta a esa build.
if(WIN32)
  add_executable(nexus-wincheck main.cpp)
  # nexus::io propaga nexus::common y nexus_options (/W4 /WX) como requisitos de uso.
  target_link_libraries(nexus-wincheck PRIVATE nexus::io)
endif()
