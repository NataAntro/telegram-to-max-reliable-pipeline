# Зависимости и ассеты

## Проверенный набор

В `requirements-dev.lock` зафиксирован набор, на котором выполнены проверки. License metadata и файлы `dist-info/licenses` локально проверены для всех установленных distributions.

| Лицензия     | Пакеты проверенного набора                                                                                                                                                            |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MIT          | aiosqlite, annotated-doc, annotated-types, anyio, FastAPI, h11, iniconfig, librt, mypy, mypy-extensions, pluggy, Pydantic, pydantic-core, pytest, Ruff, SQLAlchemy, typing-inspection |
| BSD-3-Clause | Click, httpcore, HTTPX, idna, Starlette, Uvicorn                                                                                                                                      |
| BSD-2-Clause | Pygments; Packaging как один из двух вариантов                                                                                                                                        |
| Apache-2.0   | pytest-asyncio; Packaging как один из двух вариантов                                                                                                                                  |
| MPL-2.0      | certifi, pathspec                                                                                                                                                                     |
| PSF-2.0      | typing-extensions; дополнительные включённые тексты у librt/mypy                                                                                                                      |

Все лицензии разрешительные или weak-copyleft для самостоятельного distribution-файла (`certifi`, `pathspec`); код этих пакетов не копируется в репозиторий. Перед фактической публикацией аудит повторяется по финальному lock-файлу.

## Ассеты

- `docs/assets/architecture.svg` создан специально для этого репозитория и не содержит внешних изображений, шрифтов или embedded binary data.
- `docs/assets/main-page.gif` — демонстрация главной страницы «Телеграммы Максу».
- `docs/assets/dashboard-showcase.gif` — демонстрация работы с архивом, редактором и расписанием.
- `docs/assets/pr-project.png` — изображение лендинга пиар-проекта «Трезвые перевозчики в MAX».

Продуктовые материалы предоставлены автором проекта. Для README подготовлены отдельные производные файлы; исходные PNG/MP4 в репозиторий не входят.
