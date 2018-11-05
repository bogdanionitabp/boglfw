#pragma once

#include <boglfw/utils/drawable.h>

#include <glm/vec4.hpp>
#include <glm/vec3.hpp>
#include <glm/vec2.hpp>
#include <glm/mat4x4.hpp>

#include <string>
#include <vector>

class Camera;
class Renderer;

class Viewport
{
public:
	Viewport(int x, int y, int w, int h);
	virtual ~Viewport();

	glm::vec3 bkColor() const { return backgroundColor_; }
	void setBkColor(glm::vec3 c) { backgroundColor_ = c; }
	Camera* camera() const { return pCamera_; }
	int width() const { return viewportArea_.z; }
	int height() const { return viewportArea_.w; }
	float aspect() const { return (float)width() / height(); }
	glm::vec2 position() const { return glm::vec2(viewportArea_.x, viewportArea_.y); }
	bool isEnabled() const { return enabled_; }
	bool containsPoint(glm::vec2 const&p) const;
	/**
	 * returned vector: x-X, y-Y, z-Width, w-Height
	 */
	glm::vec4 screenRect() const {return viewportArea_; }
	glm::vec3 project(glm::vec3 point) const;
	glm::vec3 unproject(glm::vec3 point) const;

	void setDrawList(std::vector<drawable> list) { drawList_.swap(list); }

	void setEnabled(bool enabled) { enabled_ = enabled; }
	void setArea(int vpX, int vpY, int vpW, int vpH);

	long userData() { return userData_; }
	void setUserData(long data) { userData_ = data; }

	std::string name() const { return name_; }
	Renderer* renderer() const { return renderer_; }

protected:
	long userData_ = 0;
	glm::vec4 viewportArea_;
	Camera* pCamera_ = nullptr;
	bool enabled_ = true;
	glm::vec3 backgroundColor_;
	std::string name_ {"unnamed"};
	std::vector<drawable> drawList_;
	Renderer* renderer_ = nullptr;

	mutable glm::mat4 mPV_cache_ {1};
	mutable glm::mat4 mPV_inv_cache_ {1};

	void setName(std::string name) { name_ = name; }
	void setRenderer(Renderer* r) { renderer_ = r; }

	friend class Renderer;
};
